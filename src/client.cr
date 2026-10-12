# Kiq::Client — pushes job payloads to Redis, 1:1 with Sidekiq::Client
# (push / push_bulk / raw_push / atomic_push). Wire-compatible: same
# LIST/ZSET/SET keys, same payload keys (jid/created_at/enqueued_at in
# epoch milliseconds, like sidekiq 8.1.x).

module Kiq
  class Client
    getter config : Config

    def initialize(@config : Config = Kiq.default_configuration)
    end

    def self.push(item : Msg) : String?
      new.push(item)
    end

    def self.push_bulk(items : Msg) : Array(String?)
      new.push_bulk(items)
    end

    # The main method used to push a job to Redis. Returns the jid, or
    # nil if a client middleware stopped the push.
    def push(item : Msg) : String?
      normed = normalize_item(item)
      payload = @config.client_middleware.invoke_client(normed["class"].to_s, normed, normed["queue"].to_s) { normed }
      return nil unless payload
      verify_json(payload)
      raw_push([payload])
      payload["jid"].to_s
    end

    # Push a large number of jobs in one round trip. `items["args"]` is
    # an Array of Arrays; every job gets its own jid.
    def push_bulk(items : Msg) : Array(String?)
      args = Client.arg_of(items, "args").as(Array(Arg))
      normed = normalize_item(items)
      jids = [] of String?
      args.each do |job_args|
        copy = normed.dup
        copy["args"] = Client.to_any(job_args.as(Arg))
        copy["jid"] = JSON::Any.new(Kiq.new_jid)
        result = @config.client_middleware.invoke_client(normed["class"].to_s, copy, copy["queue"].to_s) { copy }
        jids << (result ? result["jid"].to_s : nil)
        raw_push([result]) if result
      end
      jids
    end

    # --- item normalization (Sidekiq::JobUtil#normalize_item) ----------

    def normalize_item(item : Msg) : Msg
      raise ArgumentError.new("Job must be a Hash with 'class' and 'args' keys: `#{item}`") unless item["class"]? && item["args"]?
      klass = item["class"].to_s
      args = item["args"]?

      # merge in the class's kiq_options (compile-time registry), the
      # item itself wins — like sidekiq merges get_sidekiq_options.
      merged = Kiq.job_options(klass).dup
      item.each { |k, v| merged[k] = v }

      raise ArgumentError.new("Job must include a valid queue name") if merged["queue"]?.to_s.empty?
      merged["jid"] ||= JSON::Any.new(Kiq.new_jid)
      merged["class"] = JSON::Any.new(klass)
      merged["queue"] = JSON::Any.new(merged["queue"].to_s)
      merged["created_at"] ||= JSON::Any.new(Kiq.now_ms)
      merged
    end

    def verify_json(item : Msg) : Nil
      unsafe = json_unsafe?(Client.arg_of(item, "args"))
      return unless unsafe
      raise ArgumentError.new(
        "Job arguments to #{item["class"]} must be native JSON types, but #{unsafe} is not."
      )
    end

    # --- raw push --------------------------------------------------------

    def raw_push(payloads : Array(Msg)) : Nil
      if Testing.fake?
        payloads.each do |job|
          job["enqueued_at"] = JSON::Any.new(Kiq.now_ms) unless job["at"]?
          Queues.push(job["queue"].to_s, job["class"].to_s, job)
        end
      elsif Testing.inline?
        payloads.each do |job|
          job["id"] ||= JSON::Any.new(Kiq.new_jid)
          Kiq.process_job_inline(job)
        end
      else
        atomic_push(payloads)
      end
    end

    def atomic_push(payloads : Array(Msg)) : Nil
      redis = @config.redis_client
      if payloads.first["at"]?
        payloads.each do |hash|
          at = hash["at"].to_s
          copy = hash.dup
          copy.delete("at")
          copy.delete("enqueued_at")
          redis.zadd("schedule", at, Kiq.dump_json(copy))
        end
      else
        now = Kiq.now_ms
        grouped = payloads.group_by { |job| job["queue"].to_s }
        redis.sadd("queues", grouped.keys)
        grouped.each do |queue, jobs|
          jobs.each do |job|
            job["enqueued_at"] = JSON::Any.new(now)
            redis.lpush("queue:#{queue}", Kiq.dump_json(job))
          end
        end
      end
    end

    # Resque-compatible helpers (1:1 with Sidekiq::Client singleton).
    def self.enqueue(klass : String, *args : Arg) : String?
      push({"class" => JSON::Any.new(klass), "args" => to_any(args.to_a)})
    end

    def self.enqueue_to(queue : String, klass : String, *args : Arg) : String?
      push({"queue" => JSON::Any.new(queue), "class" => JSON::Any.new(klass), "args" => to_any(args.to_a)})
    end

    # --- Arg <-> JSON::Any ------------------------------------------------

    def self.to_any(v : Arg) : JSON::Any
      case v
      when Nil           then JSON::Any.new(nil)
      when Bool          then JSON::Any.new(v)
      when Int32, Int64  then JSON::Any.new(v.as(Int::Primitive))
      when Float64       then JSON::Any.new(v)
      when String        then JSON::Any.new(v)
      when Array         then JSON::Any.new(v.map { |x| to_any(x) })
      when Hash          then JSON::Any.new(v.transform_values { |x| to_any(x) })
      else raise ArgumentError.new("unsupported job argument type #{v.class}")
      end
    end

    def self.arg_of(msg : Msg, key : String) : Arg
      from_any(msg[key]? || JSON::Any.new([] of JSON::Any))
    end

    def self.from_any(v : JSON::Any) : Arg
      case raw = v.raw
      when Nil, Bool, String then raw.as(Arg)
      when Int64
        i = raw.as(Int64)
        i >= Int32::MIN && i <= Int32::MAX ? Int32.new(i).as(Arg) : i.as(Arg)
      when Float64 then raw.as(Arg)
      when Array   then raw.map { |x| from_any(x).as(Arg) }.as(Arg)
      when Hash    then h = {} of String => Arg; raw.each { |k, x| h[k] = from_any(x) }; h.as(Arg)
      else raise ArgumentError.new("unsupported JSON value")
      end
    end

    private def json_unsafe?(v : Arg) : String?
      case v
      when Nil, Bool, Int32, Int64, Float64, String then nil
      when Array
        v.each { |x| return "element" if json_unsafe?(x) }
        nil
      when Hash
        v.each_value { |x| return "value" if json_unsafe?(x) }
        nil
      else v.class.to_s
      end
    end
  end
end

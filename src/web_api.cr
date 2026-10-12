# Kiq process registry + Web UI data API additions — 1:1 with sidekiq's
# api.rb (ProcessSet / Process / WorkSet / JobRecord / SortedEntry /
# SortedSet / Stats::History), matching the observable Redis structures.

module Kiq
  # A single kiq process registered in Redis via the Launcher heartbeat.
  class Process
    getter data : Hash(String, JSON::Any)

    def initialize(@data)
    end

    def [](key : String) : JSON::Any?
      @data[key]?
    end

    def []?(key : String) : JSON::Any?
      @data[key]?
    end

    def identity : String
      @data["identity"].to_s
    end

    def quiet! : Nil
      signal("TSTP")
    end

    def stop! : Nil
      signal("TERM")
    end

    def dump_threads : Nil
      signal("TTIN")
    end

    def stopping? : Bool
      quiet = @data["quiet"]?
      quiet.to_s == "true"
    end

    def embedded? : Bool
      @data["embedded"]?.try(&.raw) == true
    end

    def tag : String?
      @data["tag"]?.try(&.to_s)
    end

    # View accessors (1:1 with sidekiq's Process).
    def labels : Array(String)
      case v = @data["labels"]?
      when Nil then [] of String
      else
        v.as_a?.try(&.map(&.to_s)) || [] of String
      end
    end

    def version : String?
      @data["version"]?.try(&.to_s)
    end

    def queues : Array(String)
      case v = @data["queues"]?
      when Nil then [] of String
      else
        v.as_a?.try(&.map(&.to_s)) || [] of String
      end
    end

    private def signal(sig : String) : Nil
      Kiq.default_configuration.redis_client.lpush("#{identity}-signals", sig)
    end
  end

  class ProcessSet
    include Enumerable(Process)

    def self.[](identity : String) : Process?
      redis = Kiq.default_configuration.redis_client
      exists = redis.sismember("processes", identity)
      return nil unless exists

      info, busy, beat, quiet, rss, rtt_us = redis.hmget(identity, "info", "busy", "beat", "quiet", "rss", "rtt_us")
      return nil if info.nil?

      hash = JSON.parse(info.to_s).as_h
      hash["busy"] = JSON::Any.new(busy.to_s.to_i64? || 0_i64)
      hash["beat"] = JSON::Any.new(beat.to_s.to_f64? || 0.0)
      hash["quiet"] = JSON::Any.new(quiet.to_s)
      hash["rss"] = JSON::Any.new(rss.to_s.to_i64? || 0_i64)
      hash["rtt_us"] = JSON::Any.new(rtt_us.to_s.to_i64? || 0_i64)
      Process.new(hash)
    end

    def initialize(clean : Bool = true)
      cleanup if clean
    end

    # Prune `processes` members whose heartbeat hash expired (1:1).
    def cleanup : Int64
      redis = Kiq.default_configuration.redis_client
      return 0_i64 unless redis.set("process_cleanup", "1", ex: 60, nx: true)

      procs = case v = redis.smembers("processes")
              when Array then v.map(&.to_s)
              else [] of String
              end
      return 0_i64 if procs.empty?

      heartbeats = procs.map { |key| redis.hget(key, "info").try(&.to_s) }
      to_prune = [] of String
      procs.each_with_index do |proc, i|
        to_prune << proc if heartbeats[i].nil?
      end
      redis.srem("processes", to_prune) unless to_prune.empty?
      to_prune.size.to_i64
    end

    def each : Nil
      redis = Kiq.default_configuration.redis_client
      procs = case v = redis.smembers("processes")
              when Array then v.map(&.to_s).sort
              else [] of String
              end

      procs.each do |key|
        info, concurrency, busy, beat, quiet, rss, rtt_us =
          redis.hmget(key, "info", "concurrency", "busy", "beat", "quiet", "rss", "rtt_us")
        next if info.nil?

        hash = JSON.parse(info.to_s).as_h
        hash["concurrency"] = JSON::Any.new(concurrency.to_s.to_i64? || 0_i64)
        hash["busy"] = JSON::Any.new(busy.to_s.to_i64? || 0_i64)
        hash["beat"] = JSON::Any.new(beat.to_s.to_f64? || 0.0)
        hash["quiet"] = JSON::Any.new(quiet.to_s)
        hash["rss"] = JSON::Any.new(rss.to_s.to_i64? || 0_i64)
        hash["rtt_us"] = JSON::Any.new(rtt_us.to_s.to_i64? || 0_i64)
        yield Process.new(hash)
      end
    end

    def size : Int64
      Kiq.default_configuration.redis_client.scard("processes")
    end

    def total_concurrency : Int64
      sum &.[]("concurrency").to_s.to_i64
    end

    def total_rss_in_kb : Int64
      sum &.[]("rss").to_s.to_i64
    end

    # The process with the oldest heartbeat (leader for the busy page).
    def leader : Process?
      each { |p| return p }
      nil
    end
  end

  # Jobs currently executing (best effort: recorded by the processor).
  class WorkSet
    include Enumerable(NamedTuple(process_id: String, thread_id: String, payload: Hash(String, JSON::Any)))

    def size : Int64
      to_a.size.to_i64
    end

    def each(& : NamedTuple(process_id: String, thread_id: String, payload: Hash(String, JSON::Any)) -> Nil) : Nil
      # Kiq does not register per-job work entries (P2; sidekiq stores them
      # in `<identity>:work` hashes updated around perform).
    end
  end

  # A job inside a queue LIST (sidekiq's JobRecord).
  class JobRecord
    getter value : String
    getter queue : String

    def initialize(@value : String, @queue : String)
      @item = Kiq.load_json(@value)
    end

    def [](key : String) : JSON::Any?
      @item[key]?
    end

    def jid : String
      @item["jid"].to_s
    end

    def display_class : String
      if (wrapped = @item["wrapped"]?) && !wrapped.to_s.empty?
        wrapped.to_s
      else
        @item["class"].to_s
      end
    end

    def display_args : Array(Arg)
      args = Kiq::Client.from_any(@item["args"]? || JSON::Any.new([] of JSON::Any))
      args.as?(Array(Arg)) || [] of Arg
    end

    def tags : Array(String)
      case v = @item["tags"]?
      when Nil then [] of String
      else
        Kiq::Client.from_any(v).as?(Array(Arg)).try(&.map(&.to_s)) || [] of String
      end
    end

    def delete : Nil
      Kiq.default_configuration.redis_client.lrem("queue:#{@queue}", 1, @value)
    end
  end

  # An entry inside a sorted set (sidekiq's SortedEntry).
  class SortedEntry
    getter score : Float64
    getter parent : String?
    @item : Msg

    def initialize(@parent : String?, @score : Float64, value : String)
      @item = Kiq.load_json(value)
      @value = value
    end

    def [](key : String) : JSON::Any?
      @item[key]?
    end

    def []?(key : String) : JSON::Any?
      @item[key]?
    end
    def item : Msg
      @item
    end

    def jid : String
      @item["jid"].to_s
    end

    def queue : String
      @item["queue"].to_s
    end

    def delete : Nil
      return unless (p = @parent)
      Kiq.default_configuration.redis_client.zrem(p, @value)
    end

    def klass : String
      if (wrapped = @item["wrapped"]?) && !wrapped.to_s.empty?
        wrapped.to_s
      else
        @item["class"].to_s
      end
    end

    # Move the entry back to its queue now (sidekiq SortedEntry#add_to_queue).
    def add_to_queue : Nil
      parent = @parent
      return unless parent
      remove = Kiq.default_configuration.redis_client.zrem(parent, @value)
      if remove
        msg = @item.dup
        msg.delete("at")
        Kiq::Client.new.push(msg)
      end
    end

    # Enqueue this job from its set (retry or dead) so it will be executed
    # soon; manual retries do not count against the retry limit (1:1 with
    # sidekiq: `msg["retry_count"] -= 1 if msg["retry_count"]`).
    def retry : Nil
      parent = @parent
      return unless parent
      remove = Kiq.default_configuration.redis_client.zrem(parent, @value)
      if remove
        msg = @item.dup
        msg.delete("at")
        if (rc = msg["retry_count"]?) && (n = rc.to_s.to_i?)
          msg["retry_count"] = JSON::Any.new(n - 1)
        end
        Kiq::Client.new.push(msg)
      end
    end

    # Move a retry straight to the dead set (sidekiq SortedEntry#kill).
    def kill : Nil
      parent = @parent
      return unless parent
      remove = Kiq.default_configuration.redis_client.zrem(parent, @value)
      if remove
        now = Time.utc.to_unix_f
        Kiq.default_configuration.redis_client.zadd("dead", now.to_s, @value)
      end
    end

    def value : String
      @value
    end

    # View helpers (1:1 with sidekiq's SortedEntry accessors used by the Web UI).
    def at : Time
      Time.unix(@score.to_i64)
    end

    def display_class : String
      klass
    end

    def display_args : Array(JSON::Any)
      case raw = (@item["args"]? || JSON::Any.new([] of JSON::Any)).raw
      when Array(JSON::Any::Type)
        raw.map { |v| JSON::Any.new(v) }
      else
        [] of JSON::Any
      end
    end

    def tags : Array(String)
      case v = @item["tags"]?
      when Nil then [] of String
      else
        Kiq::Client.from_any(v).as?(Array(Arg)).try(&.map(&.to_s)) || [] of String
      end
    end

    def error? : Bool
      !@item["error_class"]?.nil?
    end
  end

  # Web-UI extensions to the existing JobSet: pagination/fetch/scan with
  # sidekiq's semantics (api.rb JobSet).
  class JobSet
    # Find an entry by score + jid (sidekiq JobSet#fetch).
    def fetch(score : Float64, jid : String) : Array(SortedEntry)
      results = [] of SortedEntry
      members = redis.zrangebyscore(name, score, score)
      members = case members
                when Array then members.map(&.to_s)
                else [] of String
                end
      members.each do |member|
        entry = SortedEntry.new(name, score, member)
        results << entry if entry.jid == jid
      end
      results
    end

    # Page through the set: [current_page, total_size, entries].
    def page(page_str : String?, count : Int32, reverse : Bool = false) : {Int32, Int64, Array(SortedEntry)}
      total_size = size
      count = 1 if count < 1
      page = (page_str.try(&.to_i?) || 1)
      page = 1 if page < 1
      ((total_size / count) + 1).to_i.times { |i| return {i + 1, total_size, page_slice(i + 1, count, reverse)} if i + 1 == page }
      records = page_slice(page, count, reverse)
      {page, total_size, records}
    end

    private def page_slice(page : Int32, count : Int32, reverse : Bool) : Array(SortedEntry)
      idx = (page - 1) * count
      range = reverse ? redis.zrevrange(name, idx.to_i64, (idx + count - 1).to_i64, with_scores: true) : redis.zrange(name, idx.to_i64, (idx + count - 1).to_i64, with_scores: true)
      arr = case range
            when Array then range.map(&.to_s)
            else [] of String
            end
      entries = [] of SortedEntry
      (0...(arr.size - 1)).step(2) do |i|
        entries << SortedEntry.new(name, arr[i + 1].to_f64? || 0.0, arr[i])
      end
      entries
    end

    # Scan by substring (sidekiq JobSet#scan used by filtering).
    def scan(substr : String) : Array(SortedEntry)
      needle = substr.downcase
      entries = [] of SortedEntry
      arr = case v = redis.zrange(name, 0, -1, with_scores: true)
            when Array then v.map(&.to_s)
            else [] of String
            end
      (0...(arr.size - 1)).step(2) do |i|
        member = arr[i]
        if member.downcase.includes?(needle)
          entries << SortedEntry.new(name, arr[i + 1].to_f64? || 0.0, member)
        end
      end
      entries
    end

    # Retry all entries (RetrySet/DeadSet) — same decrement as #retry.
    def retry_all : Nil
      each(&.retry)
    end

    def kill_all : Nil
      each(&.kill)
    end

    def each(& : SortedEntry -> Nil) : Nil
      arr = case v = redis.zrange(name, 0, -1, with_scores: true)
            when Array then v.map(&.to_s)
            else [] of String
            end
      (0...(arr.size - 1)).step(2) do |i|
        yield SortedEntry.new(name, arr[i + 1].to_f64? || 0.0, arr[i])
      end
    end

    private def redis
      Kiq.default_configuration.redis_client
    end
  end

  class Stats
    def history_processed(days : Int32 = 7) : Hash(String, Int64)
      History.new(days).processed
    end

    def history_failed(days : Int32 = 7) : Hash(String, Int64)
      History.new(days).failed
    end

    # Sidekiq::Stats::History — reads stat:processed:<date> keys.
    class History
      getter days : Int32

      def initialize(@days)
      end

      def processed : Hash(String, Int64)
        stats("stat:processed")
      end

      def failed : Hash(String, Int64)
        stats("stat:failed")
      end

      private def stats(stat : String) : Hash(String, Int64)
        redis = Kiq.default_configuration.redis_client
        result = {} of String => Int64
        buff = [] of String
        dates = [] of String
        days.times do |i|
          date = (Time.utc - i.days).to_s("%Y-%m-%d")
          dates << date
          buff << "#{stat}:#{date}"
          if buff.size == 10
            read_bucket(redis, buff, dates, result)
            buff.clear
            dates.clear
          end
        end
        read_bucket(redis, buff, dates, result) unless buff.empty?
        result
      end

      private def read_bucket(redis, keys, dates, result) : Nil
        keys.each_with_index do |key, idx|
          val = redis.get(key).try(&.to_s)
          result[dates[idx]] = val.try(&.to_i64?) || 0_i64
        end
      end
    end
  end
end

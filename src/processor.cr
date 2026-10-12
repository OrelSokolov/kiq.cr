# Kiq::Processor — a worker fiber: fetch → execute → retry/death, 1:1
# with Sidekiq::Processor + Sidekiq::BasicFetch. Retry math, morgue
# semantics and stats counters are byte-compatible with sidekiq 8.1.7.

module Kiq
  # Ruby doesn't have atomic counters; Crystal fibers on one thread make
  # a plain += atomic enough (no preemption inside the increment).
  class Counter
    property value : Int64 = 0

    def incr(amount : Int64 = 1) : Nil
      @value += amount
    end

    def reset : Int64
      val = @value
      @value = 0
      val
    end
  end

  class BasicFetch
    TIMEOUT = 2

    class UnitOfWork
      getter queue : String # full key, e.g. "queue:default"
      getter job : String   # raw JSON

      def initialize(@queue, @job)
      end

      def queue_name : String
        queue.sub(/\Aqueue:/, "")
      end

      def acknowledge : Nil
      end

      def requeue : Nil
        Kiq.default_configuration.redis_client.rpush(queue, job)
      end
    end

    def initialize(@config : Config, @queues : Array(String), @strict : Bool)
    end

    def retrieve_work : UnitOfWork?
      qs = queues_cmd
      return nil if qs.empty?
      keys = qs.map { |q| "queue:#{q}" }
      result = @config.redis_client.brpop(keys, timeout: TIMEOUT)
      return nil unless result
      # brpop returns [queue, payload] or nil
      pair = result.as?(Array(::Redis::Value)).try(&.map(&.to_s))
      return nil unless pair && pair.size == 2
      UnitOfWork.new(pair[0], pair[1])
    end

    # Recreate the queue command each time to honor weights and avoid
    # queue starvation (1:1 with BasicFetch#queues_cmd).
    private def queues_cmd : Array(String)
      if @strict
        @queues.uniq
      else
        @queues.shuffle.uniq
      end
    end
  end

  class Processor
    PROCESSED = Counter.new
    FAILURE = Counter.new

    getter config : Config

    @done = false

    def initialize(@config : Config, @queues : Array(String), @strict : Bool)
      @fetch = BasicFetch.new(@config, @queues, @strict)
    end

    def stopping? : Bool
      @done
    end

    def finished? : Bool
      @finished
    end

    def terminate : Nil
      @done = true
    end

    def start : Fiber
      spawn(name: "kiq/processor") do
        begin
          run
        ensure
          @finished = true
        end
      end
    end

    @finished = false

    private def run : Nil
      until @done
        uow = begin
          fetch
        rescue ex : Exception
          # Redis/network hiccup: log through error handlers and retry,
          # like sidekiq's fetch retry loop.
          Kiq.handle_exception(ex)
          sleep(1.second) unless @done
          next
        end
        process(uow) if uow
      end
    end

    private def fetch : BasicFetch::UnitOfWork?
      j = @fetch.retrieve_work
      if j && @done
        j.requeue
        nil
      else
        j
      end
    end

    def process(uow : BasicFetch::UnitOfWork) : Nil
      jobstr = uow.job
      queue = uow.queue_name

      msg = begin
        Kiq.load_json(jobstr)
      rescue ex : JSON::Error
        # Malformed JSON goes straight to the morgue (1:1).
        send_to_morgue_raw(jobstr)
        Kiq.handle_exception(ex)
        return
      end

      begin
        execute(msg, queue)
      rescue ex : Exception
        # Infrastructure errors (e.g. unknown job class): error handlers
        # only, no retry — documented deviation (sidekiq retries these).
        FAILURE.incr
        Kiq.handle_exception(ex, msg)
      ensure
        PROCESSED.incr
      end
    end

    private def execute(msg : Msg, queue : String) : Nil
      class_name = msg["class"].to_s
      job = Kiq.build_job(class_name, msg)
      if job.nil?
        raise ArgumentError.new("uninitialized constant #{class_name}")
      end
      job.jid = msg["jid"]?.try(&.to_s)
      args = Client.arg_of(msg, "args").as(Array(Arg))
      begin
        @config.server_middleware.invoke_server(job, msg, queue) do
          Kiq.dispatch_perform(job, args)
        end
      rescue ex : Exception
        # the job raised: count the failure and create a retry / send to
        # the morgue — mirrors sidekiq's JobRetry flow
        FAILURE.incr
        process_retry(msg, queue, ex)
      end
    end

    # --- failure handling (Sidekiq::JobRetry) ----------------------------

    private def process_retry(msg : Msg, queue : String, ex : Exception) : Nil
      retry_opt = msg["retry"]?
      if retry_opt && retry_opt.raw == false
        # retry: false -> discarded, death handlers only, nothing stored
        run_death_handlers(msg, ex)
        Kiq.handle_exception(ex, msg)
        return
      end

      max_retries = retry_opt.try { |r| r.raw.as?(Int64) || r.raw.as?(Int32) } || @config.max_retries

      msg["queue"] = JSON::Any.new(msg["retry_queue"]?.try(&.to_s) || queue)
      msg["error_message"] = JSON::Any.new(ex.message.to_s[0, 10_000])
      # Crystal's `raise "string"` produces a plain Exception; Ruby's
      # produces RuntimeError. Report plain Exceptions as RuntimeError
      # for wire-compatible retry payloads.
      error_class = ex.class == Exception ? "RuntimeError" : ex.class.name
      msg["error_class"] = JSON::Any.new(error_class)

      count = if (rc = msg["retry_count"]?)
        msg["retried_at"] = JSON::Any.new(Kiq.now_ms)
        msg["retry_count"] = JSON::Any.new(rc.raw.as(Int64) + 1)
        rc.raw.as(Int64) + 1
      else
        msg["failed_at"] = JSON::Any.new(Kiq.now_ms)
        msg["retry_count"] = JSON::Any.new(0_i64)
        0_i64
      end

      if count >= max_retries
        retries_exhausted(msg, ex)
        return
      end

      # 1:1: delay = count**4 + 15, plus jitter rand(10 * (count + 1))
      delay = (count.to_i64 ** 4) + 15
      jitter = rand(10 * (count + 1)).to_f
      retry_at = Time.utc.to_unix_f + delay + jitter
      @config.redis_client.zadd("retry", retry_at.to_s, Kiq.dump_json(msg))
      Kiq.handle_exception(ex, msg)
    end

    private def retries_exhausted(msg : Msg, ex : Exception) : Nil
      discarded = msg["dead"]?.try(&.raw) == false
      unless discarded
        send_to_morgue(msg)
      end
      run_death_handlers(msg, ex)
      Kiq.handle_exception(ex, msg)
    end

    private def run_death_handlers(msg : Msg, ex : Exception) : Nil
      @config.death_handlers.each do |handler|
        begin
          handler.call(msg, ex)
        rescue ex2 : Exception
          # best effort, like sidekiq
        end
      end
    end

    def send_to_morgue(msg : Msg) : Nil
      send_to_morgue_raw(Kiq.dump_json(msg))
    end

    def send_to_morgue_raw(payload : String) : Nil
      redis = @config.redis_client
      now = Time.utc.to_unix_f
      redis.zadd("dead", now.to_s, payload)
      redis.zremrangebyscore("dead", "-inf", (now - @config.dead_timeout_in_seconds).to_s)
      redis.zremrangebyrank("dead", 0, -@config.dead_max_jobs)
    end
  end
end

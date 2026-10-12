# Kiq::Batch — 1:1 port of sidekiq-dags (Sidekiq::Batch) for kiq.cr.
#
# Source: oracle/vendor/sidekiq-dags/lib/sidekiq/batch.rb (plus status.rb,
# explicit_status.rb, final_status_snapshot.rb, callback.rb, middleware.rb,
# extension/worker.rb, batch_coordination_middleware.rb).
#
# Wire format is Redis-compatible with the original: the same
# `BID-<bid>` hashes, `-jids`/`-failed`/`-complete`/`-success` sets,
# `-callbacks-<event>` sets, `invalidated-bid-<bid>` flags, the same
# callback Worker payloads. Jobs pushed by a Ruby sidekiq-dags client run
# under Kiq and vice versa.
#
# Documented deviations (Crystal port):
# - FinalStatusSnapshot serializes to JSON, not Marshal+Base64 (the blob is
#   internal to the batch system; both producer and consumer are kiq).
# - Callback classes resolve through a compile-time registry
#   (`Kiq::Batch::CallbackHandler`, auto-registered via `included`), not
#   Object.const_get.
# - Thread.current[:batch] becomes a per-fiber current-batch stack.
# - `batch_push_interval` (incremental push) is not supported; counters are
#   flushed at run() like the default sidekiq-dags configuration.

module Kiq
  class Batch
    class NoBlockGivenError < Exception; end
    class BatchAlreadyStartedError < Exception; end

    BID_EXPIRE_TTL = 2_592_000
    CALLBACK_FLAG_TTL = 86_400

    getter bid : String
    getter description : String?
    getter callback_queue : String?
    getter created_at : Float64

    @existing : Bool
    @initialized = false
    @started = false
    @jobs_block : Proc(Nil)?
    @queued_jids = [] of String
    @pending_jids = [] of String

    def initialize(existing_bid : String? = nil)
      @bid = (existing_bid && !existing_bid.empty?) ? existing_bid : Kiq.new_batch_bid
      @existing = !existing_bid.nil? && !existing_bid.empty?
      @created_at = Time.utc.to_unix_f
      @bidkey = "BID-#{@bid}"
    end

    def description=(description : String)
      @description = description
      persist_bid_attr("description", description)
    end

    def callback_queue=(callback_queue : String)
      @callback_queue = callback_queue
      persist_bid_attr("callback_queue", callback_queue)
    end

    def callback_batch=(callback_batch : String)
      persist_bid_attr("callback_batch", callback_batch)
    end

    def on(event : Symbol, callback : Kiq::Batch::Callback::CallbackHandler.class, options : Hash(String, Arg) = {} of String => Arg) : Nil
      raise BatchAlreadyStartedError.new("Cannot add callbacks to a batch that has already been started") if @started
      return unless {"success", "complete"}.includes?(event.to_s)

      callback_key = "#{@bidkey}-callbacks-#{event}"
      name = callback.name.not_nil!
      payload = Kiq.dump_json({"callback" => JSON::Any.new(name),
                                "opts"    => Client.to_any(options)})
      redis.sadd(callback_key, payload)
      redis.expire(callback_key, BID_EXPIRE_TTL)
    end

    def add_jobs : self
      raise NoBlockGivenError.new
    end

    def add_jobs(&block : -> Nil) : self
      @jobs_block = block
      self
    end

    def run : Array(String)
      block = @jobs_block
      return [] of String unless block

      @started = true

      parent = Batch.current
      parent_bid = parent.try(&.bid)

      if !@existing && !@initialized
        redis.multi do |tx|
          tx.hset(@bidkey, {"created_at" => sprintf("%.6f", @created_at)})
          tx.expire(@bidkey, BID_EXPIRE_TTL)
          if parent_bid
            tx.hset(@bidkey, {"parent_bid" => parent_bid})
            tx.hincrby("BID-#{parent_bid}", "children", 1)
          end
        end
        @initialized = true
      end

      @queued_jids = [] of String
      @pending_jids = [] of String

      Batch.with_current(self, parent_bid) { block.call }

      # If the batch is empty (0 jobs), set pending/total/done to 0 and
      # trigger callbacks if any are registered (1:1).
      if @queued_jids.empty?
        stored_parent_bid = redis.hget(@bidkey, "parent_bid").try(&.to_s)
        stored_parent_bid = nil if stored_parent_bid && stored_parent_bid.empty?

        redis.multi do |tx|
          tx.hset(@bidkey, {"pending" => "0", "total" => "0", "done" => "0"})
          tx.expire(@bidkey, BID_EXPIRE_TTL)
          tx.expire("BID-#{stored_parent_bid}", BID_EXPIRE_TTL) if stored_parent_bid
        end

        has_complete_callback = redis.scard("#{@bidkey}-callbacks-complete").to_i64 > 0
        has_success_callback = redis.scard("#{@bidkey}-callbacks-success").to_i64 > 0

        if has_complete_callback || has_success_callback
          Batch.enqueue_callbacks(:complete, @bid)
          Batch.enqueue_callbacks(:success, @bid) if has_success_callback
        end

        return [] of String
      end

      conditional_redis_increment(true)

      redis.multi do |tx|
        tx.expire("BID-#{parent_bid}", BID_EXPIRE_TTL) if parent_bid
        tx.expire(@bidkey, BID_EXPIRE_TTL)
        tx.sadd("#{@bidkey}-jids", @queued_jids)
        tx.expire("#{@bidkey}-jids", BID_EXPIRE_TTL)
      end

      @queued_jids.dup
    end

    def increment_job_queue(jid : String) : Nil
      @queued_jids << jid
      @pending_jids << jid
      conditional_redis_increment
    end

    def conditional_redis_increment(force : Bool = false) : Nil
      return unless force # incremental push unsupported: flush at run() only

      parent_bid = Batch.current_parent_bid
      redis.multi do |tx|
        if parent_bid
          tx.hincrby("BID-#{parent_bid}", "total", @pending_jids.size)
          tx.expire("BID-#{parent_bid}", BID_EXPIRE_TTL)
        end
        tx.hincrby(@bidkey, "pending", @pending_jids.size)
        tx.hincrby(@bidkey, "total", @pending_jids.size)
        tx.expire(@bidkey, BID_EXPIRE_TTL)
      end
      @pending_jids = [] of String
    end

    def invalidate_all : Nil
      redis.multi do |tx|
        tx.set("invalidated-bid-#{@bid}", "1", ex: BID_EXPIRE_TTL)
      end
    end

    def parent_bid : String?
      v = redis.hget(@bidkey, "parent_bid").try(&.to_s)
      v.nil? || v.empty? ? nil : v
    end

    def parent : Batch?
      pbid = parent_bid
      Batch.new(pbid) if pbid
    end

    def valid?(batch : Batch = self) : Bool
      valid = redis.exists("invalidated-bid-#{batch.bid}") == 0
      p = batch.parent
      p ? (valid && valid?(p)) : valid
    end

    private def persist_bid_attr(attribute : String, value : String) : Nil
      redis.multi do |tx|
        tx.hset(@bidkey, {attribute => value})
        tx.expire(@bidkey, BID_EXPIRE_TTL)
      end
    end

    # --- per-fiber current batch (Thread.current[:batch]) ---------------

    @@current = {} of UInt64 => Batch
    @@current_parent = {} of UInt64 => String?

    def self.current : Batch?
      @@current[Fiber.current.object_id]?
    end

    def self.current_parent_bid : String?
      @@current_parent[Fiber.current.object_id]?
    end

    def self.with_current(batch : Batch?, parent_bid : String?, &)
      key = Fiber.current.object_id
      prev = @@current[key]?
      prev_parent = @@current_parent[key]?
      if batch
        @@current[key] = batch
      else
        @@current.delete(key)
      end
      @@current_parent[key] = parent_bid
      begin
        yield
      ensure
        if prev
          @@current[key] = prev
        else
          @@current.delete(key)
        end
        @@current_parent[key] = prev_parent
      end
    end

    # --- server-side hooks (run by the server middleware) ----------------

    def self.process_failed_job(bid : String, jid : String) : Nil
      with_redis_lock("batch-lock-#{bid}", timeout: 5) do
        redis.multi do |tx|
          tx.sadd("BID-#{bid}-failed", [jid])
          tx.expire("BID-#{bid}-failed", BID_EXPIRE_TTL)
        end

        results = redis.multi do |tx|
          tx.hincrby("BID-#{bid}", "pending", 0)
          tx.scard("BID-#{bid}-failed")
          tx.hincrby("BID-#{bid}", "children", 0)
          tx.scard("BID-#{bid}-complete")
          tx.hget("BID-#{bid}", "parent_bid")
          tx.hget("BID-#{bid}", "total")
          tx.hget("BID-#{bid}", "done")
        end

        r = results.not_nil!
        pending = to_i(r[0])
        failed = to_i(r[1])
        children = to_i(r[2])
        complete = to_i(r[3])
        parent_bid = r[4].try(&.to_s)

        if parent_bid && !parent_bid.to_s.empty?
          redis.multi do |tx|
            tx.sadd("BID-#{parent_bid}-failed", [jid])
            tx.expire("BID-#{parent_bid}-failed", BID_EXPIRE_TTL)
          end
        end

        enqueue_callbacks(:complete, bid) if pending == failed && children == complete
      end
    end

    def self.process_successful_job(bid : String, jid : String) : Nil
      with_redis_lock("batch-lock-#{bid}", timeout: 5) do
        results = redis.multi do |tx|
          tx.scard("BID-#{bid}-failed")
          tx.hincrby("BID-#{bid}", "pending", -1)
          tx.hincrby("BID-#{bid}", "done", 1)
          tx.hincrby("BID-#{bid}", "children", 0)
          tx.scard("BID-#{bid}-complete")
          tx.scard("BID-#{bid}-success")
          tx.hget("BID-#{bid}", "total")
          tx.hget("BID-#{bid}", "parent_bid")

          tx.srem("BID-#{bid}-failed", [jid])
          tx.srem("BID-#{bid}-jids", [jid])
          tx.expire("BID-#{bid}", BID_EXPIRE_TTL)
        end

        r = results.not_nil!
        failed = to_i(r[0])
        pending = to_i(r[1])
        done = to_i(r[2])
        children = to_i(r[3])
        complete = to_i(r[4])
        success = to_i(r[5])

        all_success = pending.zero? && children == success
        # if complete or successfull call complete callback (the complete
        # callback may then call successful)
        if (pending == failed && children == complete) || all_success
          enqueue_callbacks(:complete, bid)
          enqueue_callbacks(:success, bid) if all_success
        end
      end
    end

    def self.enqueue_callbacks(event : Symbol, bid : String) : Nil
      event_name = event.to_s
      batch_key = "BID-#{bid}"
      callback_key = "#{batch_key}-callbacks-#{event_name}"
      processed_flag_key = "#{batch_key}-processed-#{event_name}"

      with_redis_lock("callback-lock-#{bid}-#{event_name}", timeout: 5) do
        already_processed = redis.get(processed_flag_key).try(&.to_s)

        if already_processed == "true"
          next
        end

        results = redis.multi do |tx|
          tx.hget(batch_key, "pending")
          tx.hget(batch_key, "children")
          tx.scard("#{batch_key}-complete")
          tx.scard("#{batch_key}-success")
          tx.scard("#{batch_key}-failed")
        end
        r = results.not_nil!
        pending = to_i(r[0])
        children = to_i(r[1])
        complete = to_i(r[2])
        success = to_i(r[3])
        failed = to_i(r[4])

        if event_name == "complete" && (pending != failed || children != complete)
          next
        end

        if event_name == "success" && (pending != 0 || children != success)
          next
        end

        results2 = redis.multi do |tx|
          tx.smembers(callback_key)
          tx.hget(batch_key, "callback_queue")
          tx.hget(batch_key, "parent_bid")
          tx.hget(batch_key, "callback_batch")
        end
        r2 = results2.not_nil!
        callbacks = array_of_strings(r2[0])
        queue = r2[1].try(&.to_s)
        parent_bid = r2[2].try(&.to_s)
        callback_batch = r2[3].try(&.to_s)

        redis.set(processed_flag_key, "true", ex: CALLBACK_FLAG_TTL)

        if callbacks.empty?
          unless callback_batch && !callback_batch.empty?
            finalizer = Batch::Callback::Finalize.new
            status = Status.new(bid)
            finalizer.dispatch(status, {"bid" => bid, "event" => event_name})
          end
          next
        end

        queue = "default" if queue.nil? || queue.empty?
        parent_bid = nil if parent_bid.nil? || parent_bid.empty?
        callback_args = callbacks.map do |jcb|
          cb = Kiq.load_json(jcb)
          clazz = cb["callback"].to_s
          opts = cb["opts"]? || JSON::Any.new({} of String => JSON::Any)
          serialized_status = FinalStatusSnapshot.new(bid).serialized
          [JSON::Any.new(clazz), JSON::Any.new(event_name), opts, JSON::Any.new(bid), JSON::Any.new(parent_bid), JSON::Any.new(serialized_status)]
        end

        opts = {"bid" => JSON::Any.new(bid), "event" => JSON::Any.new(event_name)}

        redis.del(callback_key)

        # Run callback batch finalize synchronously
        if callback_batch && !callback_batch.empty?
          cb_opts = Client.from_any(callback_args.first[2]).as(Hash(String, Arg))
          finalizer = Batch::Callback::Finalize.new
          status = Status.new(bid)
          finalizer.dispatch(status, {"bid" => bid, "event" => event_name}.merge(cb_opts.transform_keys(&.to_s)))
          next
        end

        if callback_args.empty?
          finalizer = Batch::Callback::Finalize.new
          status = Status.new(bid)
          finalizer.dispatch(status, {"bid" => bid, "event" => event_name})
        else
          # Otherwise finalize in sub batch complete callback
          cb_batch = Batch.new
          cb_batch.callback_batch = "true"
          cb_batch.on(:complete, Batch::Callback::Finalize, {"bid" => bid, "event" => event_name})
          cb_batch.add_jobs do
            push_callbacks callback_args, queue.not_nil!
          end
          cb_batch.run
        end
      end
    end

    # Distributed Redis lock (SET NX EX with token-checked release), 1:1.
    def self.with_redis_lock(lock_key : String, timeout : Int32 = 30, max_wait : Int32 = 60, &) : Nil
      lock_token = Random::Secure.hex(16)
      locked = false
      start_time = Time.monotonic

      begin
        loop do
          locked = redis.set(lock_key, lock_token, ex: timeout, nx: true) ? true : false
          break if locked

          if (Time.monotonic - start_time).total_seconds > max_wait
            raise "Failed to acquire Redis lock '#{lock_key}' after #{max_wait} seconds"
          end

          sleep(0.0001 + rand(0.0001))
        end

        yield
      ensure
        if locked
          current_token = redis.get(lock_key).try(&.to_s)
          redis.del(lock_key) if current_token == lock_token
        end
      end
    end

    def self.push_callbacks(args : Array(Array(JSON::Any)), queue : String) : Nil
      return if args.empty?

      Kiq::Client.push_bulk({
        "class" => JSON::Any.new("Kiq::Batch::Callback::Worker"),
        "args"  => JSON::Any.new(args.map { |a| JSON::Any.new(a) }),
        "queue" => JSON::Any.new(queue),
      })
    end

    # nil.to_i == 0 semantics for Redis reads
    def self.to_i(v) : Int64
      case v
      when Int64 then v
      when Nil then 0_i64
      else v.to_s.to_i64? || 0_i64
      end
    end

    private def self.array_of_strings(v) : Array(String)
      case v
      when Array then v.map(&.to_s)
      else [] of String
      end
    end

    private def self.redis
      Kiq.default_configuration.redis_client
    end

    private def redis
      Kiq.default_configuration.redis_client
    end

    # --- Status ------------------------------------------------------------

    class Status
      getter bid : String

      def initialize(@bid)
      end

      def join : Nil
        raise "Not supported"
      end

      def pending : Int64
        redis.hget("BID-#{bid}", "pending").try(&.to_s.to_i64) || 0_i64
      end

      def failures : Int64
        redis.scard("BID-#{bid}-failed").to_s.to_i64
      end

      def created_at : String?
        redis.hget("BID-#{bid}", "created_at").try(&.to_s)
      end

      def total : Int64
        redis.hget("BID-#{bid}", "total").try(&.to_s.to_i64) || 0_i64
      end

      def done : Int64
        redis.hget("BID-#{bid}", "done").try(&.to_s.to_i64) || 0_i64
      end

      def max : Int64
        pending + done
      end

      def parent_bid : String?
        v = redis.hget("BID-#{bid}", "parent_bid").try(&.to_s)
        v.nil? || v.empty? ? nil : v
      end

      def failure_info : Array(String)
        case members = redis.smembers("BID-#{bid}-failed")
        when Array then members.map(&.to_s)
        else [] of String
        end
      end

      def complete? : Bool
        redis.hget("BID-#{bid}", "complete").try(&.to_s) == "true"
      end

      def child_count : Int64
        redis.hget("BID-#{bid}", "children").try(&.to_s.to_i64) || 0_i64
      end

      def exists? : Bool
        redis.exists("BID-#{bid}") != 0
      end

      def data : Hash(String, Arg)
        {
          "bid"          => bid,
          "total"        => total,
          "failures"     => failures,
          "pending"      => pending,
          "done"         => done,
          "max"          => max,
          "created_at"   => created_at,
          "complete"     => complete?,
          "failure_info" => failure_info,
          "parent_bid"   => parent_bid,
          "child_count"  => child_count,
        }
      end

      private def redis
        Kiq.default_configuration.redis_client
      end
    end

    # ExplicitStatus: string-typed readers (1:1 with the original).
    class ExplicitStatus < Status
      def pending : String?
        redis.hget("BID-#{bid}", "pending").try(&.to_s)
      end

      def failures : String?
        redis.scard("BID-#{bid}-failed").try(&.to_s)
      end

      def total : String?
        redis.hget("BID-#{bid}", "total").try(&.to_s)
      end

      def child_count : String?
        redis.hget("BID-#{bid}", "children").try(&.to_s)
      end

      def failure_info : Array(String)
        previous_def
      end
    end

    # Status snapshot serialized into callback args. JSON instead of
    # Marshal+Base64 (documented deviation).
    class FinalStatusSnapshot
      property bid : String
      property total : Int64
      property failures : Int64
      property pending : Int64
      property created_at : String?
      property complete : Bool
      property failure_info : Array(String)
      property parent_bid : String?
      property child_count : Int64

      def initialize(@bid)
        status = Status.new(@bid)
        @bid = status.bid
        @total = status.total
        @failures = status.failures
        @pending = status.pending
        @created_at = status.created_at
        @complete = status.complete?
        @failure_info = status.failure_info
        @parent_bid = status.parent_bid
        @child_count = status.child_count
      end

      def initialize(*, @bid, @total, @failures, @pending, @created_at,
                     @complete, @failure_info, @parent_bid, @child_count)
      end

      def complete? : Bool
        @complete
      end

      def serialized : String
        {
          bid: @bid, total: @total, failures: @failures, pending: @pending,
          created_at: @created_at, complete: @complete, failure_info: @failure_info,
          parent_bid: @parent_bid, child_count: @child_count,
        }.to_json
      end

      def self.deserialize(serialized : String) : self
        h = JSON.parse(serialized).as_h
        new(
          bid: h["bid"].to_s,
          total: h["total"]?.try(&.to_s.to_i64) || 0_i64,
          failures: h["failures"]?.try(&.to_s.to_i64) || 0_i64,
          pending: h["pending"]?.try(&.to_s.to_i64) || 0_i64,
          created_at: h["created_at"]?.try(&.to_s),
          complete: h["complete"]?.try(&.raw) == true,
          failure_info: (h["failure_info"]?.try(&.as_a?) || [] of JSON::Any).map(&.to_s),
          parent_bid: h["parent_bid"]?.try(&.to_s),
          child_count: h["child_count"]?.try(&.to_s.to_i64) || 0_i64,
        )
      end

      def data : Hash(String, Arg)
        {
          "bid"         => @bid, "total" => @total, "failures" => @failures,
          "pending"     => @pending, "created_at" => @created_at,
          "complete"    => @complete, "failure_info" => @failure_info,
          "parent_bid"  => @parent_bid, "child_count" => @child_count,
        }
      end

      def exists? : Bool
        true
      end
    end
  end

  def self.new_batch_bid : String
    bytes = Bytes.new(10)
    Random::Secure.random_bytes(bytes)
    Base64.urlsafe_encode(bytes, padding: false)
  end
end

require "./batch/callback"
require "./batch/middleware"

# Auto-configure the batch middleware, exactly what requiring
# sidekiq-dags does (client chain now; server chain at Launcher boot).
Kiq::Batch::Middleware.configure
Kiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add Kiq::BatchCoordinationClientMiddleware.new unless chain.exists?(Kiq::BatchCoordinationClientMiddleware)
  end
end
Kiq.configure_server do |config|
  config.server_middleware do |chain|
    chain.add Kiq::BatchCoordinationServerMiddleware.new unless chain.exists?(Kiq::BatchCoordinationServerMiddleware)
  end
  config.client_middleware do |chain|
    chain.add Kiq::BatchCoordinationClientMiddleware.new unless chain.exists?(Kiq::BatchCoordinationClientMiddleware)
  end
end

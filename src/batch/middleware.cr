# Kiq::Batch::Middleware + coordination middleware (sidekiq-dags
# lib/sidekiq/batch/middleware.rb + batch_coordination_middleware.rb).

module Kiq
  class Batch
    module Middleware
      # Attaches the current batch's bid to pushed jobs and counts them.
      class ClientMiddleware
        include Kiq::Middleware::ClientMiddleware

        def call(job_class : String, msg : Msg, queue : String, & : -> Msg?) : Msg?
          if (batch = Batch.current)
            msg["bid"] = JSON::Any.new(batch.bid)
            batch.increment_job_queue(msg["jid"].to_s)
          end
          yield
        end
      end

      # Wraps job execution: on success/failure updates the batch counters
      # and possibly fires callbacks (1:1).
      class ServerMiddleware
        include Kiq::Middleware::ServerMiddleware

        def call(job : Kiq::AnyJob, msg : Msg, queue : String, & : -> Nil) : Nil
          if (bid_any = msg["bid"]?) && !(bid = bid_any.to_s).empty?
            begin
              Batch.with_current(Batch.new(bid), nil) do
                yield
              end
              Batch.process_successful_job(bid, msg["jid"].to_s)
            rescue ex : Exception
              Batch.process_failed_job(bid, msg["jid"].to_s)
              raise ex
            end
          else
            yield
          end
        end
      end

      def self.configure : Nil
        Kiq.configure_client do |config|
          config.client_middleware do |chain|
            chain.add ClientMiddleware.new unless chain.exists?(ClientMiddleware)
          end
        end
        Kiq.configure_server do |config|
          config.client_middleware do |chain|
            chain.add ClientMiddleware.new unless chain.exists?(ClientMiddleware)
          end
          config.server_middleware do |chain|
            chain.add ServerMiddleware.new unless chain.exists?(ServerMiddleware)
          end
        end
      end
    end
  end

  # Batch coordination: tracks real (non-Dummy) job registration and makes
  # DummyJob wait until all real jobs of the batch finished (1:1 with
  # sidekiq-dags BatchCoordination*Middleware).
  class BatchCoordinationClientMiddleware
    include Kiq::Middleware::ClientMiddleware

    def call(job_class : String, msg : Msg, queue : String, & : -> Msg?) : Msg?
      if (batch = Batch.current)
        batch_id = batch.bid

        unless dummy_job?(job_class)
          result = yield

          if msg["bid"]?.try(&.to_s) == batch_id
            meta_key = "batch-#{batch_id}-coordination"
            redis.hincrby(meta_key, "total_registered", 1)
            redis.hset(meta_key, {"last_registered_at" => Time.utc.to_unix_f.to_s})
            redis.expire(meta_key, 3600)
          end

          result
        else
          yield
        end
      else
        yield
      end
    end

    private def dummy_job?(name : String?) : Bool
      return true if name.nil?
      name == "DummyJob" || name == "Kiq::DummyJob" ||
        name == "Sidekiq::DummyJob" || name.ends_with?("::DummyJob")
    end

    private def redis
      Kiq.default_configuration.redis_client
    end
  end

  class BatchCoordinationServerMiddleware
    include Kiq::Middleware::ServerMiddleware

    MAX_WAIT = 30
    STABILITY_WINDOW = 0.5
    POLL_INTERVAL = 0.1

    def call(job : Kiq::AnyJob, msg : Msg, queue : String, & : -> Nil) : Nil
      batch_id = msg["bid"]?.try(&.to_s)
      worker_name = job.class.name.to_s
      is_dummy = dummy_job?(worker_name)
      meta_key = batch_id ? "batch-#{batch_id}-coordination" : nil

      if batch_id && is_dummy && meta_key
        wait_for_real_jobs_completion(batch_id, meta_key)
      end

      begin
        yield
      ensure
        if batch_id && !is_dummy && meta_key
          redis.hincrby(meta_key, "executed_jobs", 1)
        end
      end
    end

    private def dummy_job?(name : String?) : Bool
      return true if name.nil?
      name == "DummyJob" || name == "Kiq::DummyJob" ||
        name == "Sidekiq::DummyJob" || name.ends_with?("::DummyJob")
    end

    private def wait_for_real_jobs_completion(batch_id : String, meta_key : String) : Nil
      start_time = Time.monotonic
      last_registered_at : Float64? = nil

      loop do
        total_registered, executed_jobs, last_reg = redis.hmget(meta_key, "total_registered", "executed_jobs", "last_registered_at")
        tr = total_registered.to_s.to_i64? || 0_i64
        ej = executed_jobs.to_s.to_i64? || 0_i64
        lr = last_reg.try(&.to_s)

        last_registered_at = lr.try(&.to_f64?) || last_registered_at

        # If no real jobs were registered - proceed immediately
        return if tr == 0

        # If all registered jobs finished - proceed
        return if ej >= tr

        # Stability window: no new registrations and all done
        elapsed = (Time.monotonic - start_time).total_seconds
        if last_registered_at && elapsed > STABILITY_WINDOW
          time_since_last_reg = Time.utc.to_unix_f - last_registered_at.not_nil!
          return if time_since_last_reg >= STABILITY_WINDOW && ej >= tr
        end

        return if elapsed > MAX_WAIT

        sleep(POLL_INTERVAL)
      end
    end

    private def redis
      Kiq.default_configuration.redis_client
    end
  end
end

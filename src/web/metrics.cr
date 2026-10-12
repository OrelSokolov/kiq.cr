# Kiq::Web::Metrics — counts-only port of sidekiq's Metrics tracking
# (sidekiq/metrics/tracking.rb + metrics/query.rb).
#
# Deviation from the original: Kiq records pass/fail counts per minute
# (`stat:<class>:<date>` hashes with `p<minute>`/`f<minute>` fields, exactly
# like the original) but does not record execution-time histograms, so the
# Web UI metrics tables stay empty (no hist markers in Redis).

module Kiq
  class Web
    module Metrics
      # Server middleware: increment the per-minute pass/fail counters.
      # Not registered in the default chain; apps opt in with
      # `Kiq.server { |c| c.use Kiq::Web::Metrics::Tracking.new }`.
      class Tracking
        include Kiq::Middleware::ServerMiddleware

        def call(job : Kiq::AnyJob, msg : Msg, queue : String, & : -> Nil) : Nil
          yield
          track(job.class.name, 'p')
        rescue ex
          track(job.class.name, 'f')
          raise ex
        end

        private def track(class_name : String, kind : Char) : Nil
          key = "stat:#{class_name}:#{Time.utc.to_s("%Y-%m-%d")}"
          minute = Time.utc.minute # minute-of-hour bucket, like the original
          Kiq.default_configuration.redis_client.hincrby(key, "#{kind}#{minute}", 1)
        rescue
          # metrics must never break job execution
        end
      end

      # Counts-only query API. The original returns job aggregates + time
      # series from `stat:*` keys; until the histogram tables are rendered
      # in the UI this stays a stub with the same shape.
      class Query
        def top_jobs(class_filter : String? = nil) : QueryResult
          QueryResult.new
        end

        def for_job(name : String) : JobResult
          JobResult.new(name)
        end
      end

      class QueryResult
        getter marks : Array(String) = [] of String
        getter job_results : Hash(String, JobResult) = {} of String => JobResult

        def totals : {Int64, Int64}
          {0_i64, 0_i64}
        end

        def series : Array(Int64)
          [] of Int64
        end

        def start_time : Time
          Time.utc
        end
      end

      class JobResult
        getter name : String

        def initialize(@name)
        end

        def totals : {Int64, Int64}
          {0_i64, 0_i64}
        end

        def series : Array(Int64)
          [] of Int64
        end
      end
    end
  end
end

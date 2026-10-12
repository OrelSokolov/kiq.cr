# ActiveJob adapter for the Kiq queue — the analogue of Rails'
# Sidekiq adapter (Sidekiq::QueueAdapters::SidekiqAdapter), wired into
# active_job.cr's `ActiveJob::Adapter` port. Separate require so Kiq
# itself has no hard ActiveJob dependency at runtime beyond the shard
# dependency graph.

require "./kiq_cr"
require "active_job_cr"

module ActiveJob
  class KiqAdapter
    include ActiveJob::Adapter

    # The wrapper pushed onto Redis: `class` is the wrapper (like
    # sidekiq's JobWrapper), `wrapped` carries the real job class.
    class JobWrapper
      include Kiq::Job
      kiq_options queue: "default", retry: false

      def perform(job_data : Kiq::Arg)
        hash = {} of String => ArgValue
        job_data.as(Hash(String, Kiq::Arg)).each do |k, v|
          hash[k] = kiq_arg_to_aj(v)
        end
        ActiveJob::Base.deserialize(hash).perform_now
      end

      private def kiq_arg_to_aj(v : Kiq::Arg) : ArgValue
        case v
        when Nil, Bool, String then v.as(ArgValue)
        when Int32, Int64, Float64 then v.as(ArgValue)
        when Array             then v.map { |x| kiq_arg_to_aj(x).as(ArgValue) }
        when Hash              then h = {} of String | Symbol => ArgValue; v.each { |k, x| h[k] = kiq_arg_to_aj(x) }; h.as(ArgValue)
        else raise ArgumentError.new("unsupported argument type #{v.class}")
        end
      end
    end

    def enqueue(record : EnqueuedJob) : Nil
      item = {
        "class"   => JSON::Any.new("ActiveJob::KiqAdapter::JobWrapper"),
        "wrapped" => JSON::Any.new(record.job_class),
        "queue"   => JSON::Any.new(record.queue_name),
        "args"    => Kiq::Client.to_any([build_job_data(record)] of Kiq::Arg),
        "retry"   => JSON::Any.new(false),
      } of String => JSON::Any
      if scheduled = record.scheduled_at
        ts = scheduled.to_utc.to_unix_f
        item["at"] = JSON::Any.new(ts) if ts > Time.utc.to_unix_f
      end
      Kiq::Client.new.push(item)
    end

    private def build_job_data(record : EnqueuedJob) : Kiq::Arg
      hash = {} of String => Kiq::Arg
      record.to_job_data.each do |k, v|
        hash[k] = aj_to_kiq_arg(v)
      end
      hash
    end

    private def aj_to_kiq_arg(v) : Kiq::Arg
      case v
      when Nil, Bool, String, Int32, Int64, Float64 then v.as(Kiq::Arg)
      when Symbol, Date, Time, Time::Span then v.to_s
      when Array                                 then v.map { |x| aj_to_kiq_arg(x).as(Kiq::Arg) }
      when Hash                                  then h = {} of String => Kiq::Arg; v.each { |k, x| h[k.to_s] = aj_to_kiq_arg(x) }; h
      else v.to_s
      end
    end
  end
end

# Kiq::Job — the worker DSL, 1:1 with Sidekiq::Job.
#
#   class HardJob
#     include Kiq::Job
#     kiq_options queue: "critical", retry: 5
#
#     def perform(*args)
#       # do some work
#     end
#   end
#
#   HardJob.perform_async(1, 2, 3)
#
# Deviation from Ruby: job classes are resolved through a compile-time
# registry (no runtime const-get), so every job class must be compiled
# into the binary — in a compiled language it always is.

module Kiq
  module Job
    # Configure this job class (1:1 with sidekiq_options):
    #
    #   kiq_options queue: "critical", retry: 5, dead: true,
    #                retry_queue: "low"
    macro kiq_options(**opts)
      {% ::Kiq::OPTIONS[@type.stringify] = opts %}
    end

    macro included
      {% ::Kiq::JOBS << @type %}

      # --- testing API (Sidekiq::Job::ClassMethods) --------------------
      def self.jobs : Array(::Kiq::Msg)
        ::Kiq::Queues.jobs_for({{ @type.stringify }})
      end

      def self.clear : Nil
        ::Kiq::Queues.clear_for({{ @type.stringify }})
      end

      def self.drain : Nil
        while (next_job = jobs.first?)
          ::Kiq::Queues.delete_for(next_job)
          ::Kiq.process_job_inline(next_job)
        end
      end

      def self.perform_one : Nil
        next_job = jobs.first?
        raise Kiq::EmptyQueueError.new("perform_one called with empty job queue") unless next_job
        ::Kiq::Queues.delete_for(next_job)
        ::Kiq.process_job_inline(next_job)
      end

      # --- enqueue API ---------------------------------------------------
      # NB: a typed splat in Crystal requires >= 1 argument, so the
      # common zero-arg form gets its own overload.
      def self.perform_async : String? | Bool
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_async
      end

      def self.perform_async(*args : ::Kiq::Arg) : String? | Bool
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_async(*args)
      end

      def self.perform_in(interval : Float64 | Int32 | ::Time::Span | ::Time) : String?
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_in(interval)
      end

      def self.perform_in(interval : Float64 | Int32 | ::Time::Span | ::Time, *args : ::Kiq::Arg) : String?
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_in(interval, *args)
      end

      def self.perform_at(interval : Float64 | Int32 | ::Time::Span | ::Time) : String?
        perform_in(interval)
      end

      def self.perform_at(interval : Float64 | Int32 | ::Time::Span | ::Time, *args : ::Kiq::Arg) : String?
        perform_in(interval, *args)
      end

      def self.perform_bulk(args : Array(Array(::Kiq::Arg))) : Array(String?)
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_bulk(args)
      end

      # Explicit inline execution, no queue (1:1 with perform_inline).
      def self.perform_inline : Bool
        ::Kiq.perform_inline({{ @type.stringify }}, [] of ::Kiq::Arg)
      end

      def self.perform_inline(*args : ::Kiq::Arg) : Bool
        ::Kiq.perform_inline({{ @type.stringify }}, args.to_a)
      end

      def self.set(**opts) : ::Kiq::Job::Setter
        ::Kiq::Job::Setter.new({{ @type.stringify }}).set(**opts)
      end
    end

    property jid : String? = nil

    def logger : ::Log
      Kiq.default_configuration.logger
    end
  end

  # Sidekiq::Worker compatibility alias (sidekiq 6.3+).
  Worker = Job

  class EmptyQueueError < Exception; end

  # Helper class encapsulating options for `set`, 1:1 with
  # Sidekiq::Job::Setter.
  class Job::Setter
    def initialize(@class_name : String, @opts : Msg = {} of String => JSON::Any)
    end

    def set(**opts) : self
      merge_opts(opts)
      apply_wait
      self
    end

    def perform_async : String? | Bool
      push({"args" => ::Kiq::Client.to_any([] of ::Kiq::Arg)}, {} of String => JSON::Any)
    end

    def perform_async(*args : ::Kiq::Arg) : String? | Bool
      if @opts["sync"]? == JSON::Any.new(true)
        Kiq.perform_inline(@class_name, args.to_a.map { |x| x.as(::Kiq::Arg) })
      else
        push({"args" => ::Kiq::Client.to_any(args.to_a.map { |x| x.as(::Kiq::Arg) })}, {} of String => JSON::Any)
      end
    end

    def perform_inline : Bool
      Kiq.perform_inline(@class_name, [] of ::Kiq::Arg)
    end

    def perform_inline(*args : ::Kiq::Arg) : Bool
      Kiq.perform_inline(@class_name, args.to_a.map { |x| x.as(::Kiq::Arg) })
    end

    def perform_bulk(args : Array(Array(::Kiq::Arg))) : Array(String?)
      args.map do |job_args|
        push({"args" => ::Kiq::Client.to_any(job_args)}, {} of String => JSON::Any)
      end
    end

    def perform_in(interval : Float64 | Int32 | ::Time::Span | ::Time) : String?
      opts = @opts.dup
      ts = at_timestamp(interval)
      opts["at"] = JSON::Any.new(ts) if ts
      push({"args" => ::Kiq::Client.to_any([] of ::Kiq::Arg)}, opts)
    end

    def perform_in(interval : Float64 | Int32 | ::Time::Span | ::Time, *args : ::Kiq::Arg) : String?
      opts = @opts.dup
      ts = at_timestamp(interval)
      opts["at"] = JSON::Any.new(ts) if ts
      push({"args" => ::Kiq::Client.to_any(args.to_a.map { |x| x.as(::Kiq::Arg) })}, opts)
    end

    def perform_at(interval : Float64 | Int32 | ::Time::Span | ::Time) : String?
      perform_in(interval)
    end

    def perform_at(interval : Float64 | Int32 | ::Time::Span | ::Time, *args : ::Kiq::Arg) : String?
      perform_in(interval, *args)
    end

    def push(overrides : ::Kiq::Msg, opts : ::Kiq::Msg) : String?
      item = @opts.merge(opts)
      overrides.each { |k, v| item[k] = v }
      item["class"] = JSON::Any.new(@class_name)
      ::Kiq::Client.new.push(item)
    end

    private def merge_opts(opts) : Nil
      opts.each do |k, v|
        case v
        when String  then @opts[k.to_s] = JSON::Any.new(v)
        when Bool    then @opts[k.to_s] = JSON::Any.new(v)
        when Int32   then @opts[k.to_s] = JSON::Any.new(v)
        when Float64 then @opts[k.to_s] = JSON::Any.new(v)
        when Nil     then @opts[k.to_s] = JSON::Any.new(nil)
        else
          raise ArgumentError.new("unsupported `set` option #{k}: #{v.class}")
        end
      end
    end

    # wait / wait_until (ActiveJob compatibility), 1:1 with Setter#at:
    # a small value is a relative interval, a big one an absolute epoch.
    private def apply_wait : Nil
      w = @opts.delete("wait")
      wu = @opts.delete("wait_until")
      interval = w || wu
      return unless interval
      ts = at_timestamp(interval.raw.as(Float64 | Int32))
      @opts["at"] = JSON::Any.new(ts) if ts
    end

    private def at_timestamp(interval : Float64 | Int32 | Time::Span | Time) : Float64?
      int = case interval
            when Float64        then interval
            when Int32          then interval.to_f
            when ::Time::Span   then interval.total_seconds
            when ::Time         then interval.to_unix_f
            else raise ArgumentError.new("bad interval #{interval.class}")
            end
      now = Time.utc.to_unix_f
      ts = int < 1_000_000_000 ? now + int : int
      ts > now ? ts : nil
    end
  end

  # Execute a job inline through both middleware chains without Redis
  # (1:1 with Sidekiq::Job::Setter#perform_inline).
  def self.perform_inline(class_name : String, args : Array(Arg)) : Bool
    item = {
      "class" => JSON::Any.new(class_name),
      "args"  => Client.to_any(args.to_a),
      "queue" => JSON::Any.new("default"),
      "jid"   => JSON::Any.new(Kiq.new_jid),
    } of String => JSON::Any
    item = item.merge(job_options(class_name))
    process_job_inline(item)
  end

  def self.process_job_inline(msg : Msg) : Bool
    job = build_job(msg["class"].to_s, msg)
    return false unless job
    job.jid = msg["jid"]?.try(&.to_s)
    args = Client.arg_of(msg, "args").as(Array(Arg))
    default_configuration.server_middleware.invoke_server(job, msg, msg["queue"].to_s) do
      Kiq.dispatch_perform(job, args)
    end
    true
  end

  # Job-class options lookup through the compile-time registry; unknown
  # classes get the global defaults (1:1 with JobUtil#normalized_hash).
  def self.job_options(class_name : String) : Msg
    defaults = default_configuration.default_job_options
    {% for j in ::Kiq::JOBS %}
      {% opts = ::Kiq::OPTIONS[j.stringify] %}
      {% if opts %}
        if class_name == {{ j.stringify }}
          item = defaults.dup
          {% for k, v in opts %}
            item[{{ k.stringify }}] = JSON::Any.new({{ v }})
          {% end %}
          return item
        end
      {% end %}
    {% end %}
    defaults
  end

  # Build a job instance by class name (compile-time registry).
  def self.build_job(class_name : String, msg : Msg) : AnyJob
    {% for j in ::Kiq::JOBS %}
      if class_name == {{ j.stringify }}
        return {{ j.id }}.new
      end
    {% end %}
    nil
  end

  # Dispatch `perform` with runtime args into the job's own arity,
  # generated per job class (same technique as active_job.cr: splat into
  # a def requires static arity, so valid call arities are derived from
  # the job's own perform signature at compile time).
  def self.dispatch_perform(job : AnyJob, args : Array(Arg)) : Nil
    job.__kiq_perform(args)
  end
end

# Every job class gets a generated __kiq_perform / __kiq_set_args; this
# must run at the end of the program when all perform signatures are
# known (same as active_job.cr).
macro finished
  # Union of all registered job classes: the static type flowing through
  # server middleware and perform dispatch.
  module Kiq
    {% if ::Kiq::JOBS.empty? %}
      alias AnyJob = Nil
    {% else %}
      alias AnyJob = Nil | Union({{ ::Kiq::JOBS.splat }})
    {% end %}
  end

  {% for j in ::Kiq::JOBS %}
    {% has_perform = false %}
    {% is_splat = false %}
    {% min_args = 0 %}
    {% max_args = 0 %}
    # own methods first, then ancestors (e.g. Kiq::Node subclasses
    # inherit #perform)
    {% perform_sources = [j] + j.ancestors %}
    {% for src in perform_sources %}
      {% for m in src.methods %}
        {% if m.name.stringify == "perform" && !has_perform %}
          {% has_perform = true %}
          {% is_splat = !m.splat_index.nil? %}
          {% max_args = m.args.size %}
          {% cnt = 0 %}
          {% for a in m.args %}
            {% cnt += 1 if a.default_value.is_a?(Nop) %}
          {% end %}
          {% min_args = cnt %}
        {% end %}
      {% end %}
    {% end %}
    {% if has_perform %}
      {% if is_splat %}
        {% max_args = 16 %} # a splat perform accepts any arity; cap the dispatch
        {% min_args = 0 %}  # a splat perform accepts zero args
      {% end %}
      class {{ j.id }}
        def __kiq_perform(args : Array(::Kiq::Arg)) : Nil
          n = args.size
          {% for i in 0..max_args %}
            {% if i >= min_args %}
              return perform({% for k in 0...i %}args[{{ k }}], {% end %}) if n == {{ i }}
            {% end %}
          {% end %}
          raise ArgumentError.new("Wrong number of arguments for #{self.class}#perform: #{n}")
        end
      end
    {% else %}
      class {{ j.id }}
        def __kiq_perform(args : Array(::Kiq::Arg)) : Nil
          raise ArgumentError.new("{{ j.id }} does not define #perform")
        end
      end
    {% end %}
  {% end %}
end

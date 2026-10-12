# Kiq::Node + NodeDsl + PipelineCallback — the DAG layer of sidekiq-dags
# (lib/sidekiq/{node,node_dsl,pipeline_callback,pipeline_tracking,dummy_job}.rb).
#
# Plain nodes (no tracking):
#
#   class ChainA < Kiq::Node
#     desc "A"
#     execute { LogJob.perform_async("chain:a") }
#     next_node :ChainB
#   end
#
# Pipeline DSL (compile-time, statuses tracked via Kiq::PipelineStatus).
# NB: Crystal macros are not inherited via `extend`, so the DSL macros are
# invoked with an explicit receiver:
#
#   module MyApp
#     Kiq::NodeDsl.pipeline :data_processing do
#       node :start_node do
#         desc "Start"
#         execute { StartJob.perform_async }
#         next_node :end_node
#       end
#       node :end_node do
#         desc "End"
#         execute { FinishJob.perform_async }
#       end
#     end
#   end
#
# Deviation: `next_node :x` resolves the sibling class at compile time
# (the original's runtime const_get is broken for namespaced pipelines —
# the original only chains nodes that live directly in one module).

require "./batch"
require "./dags"

# DummyJob keeps node batches non-empty (1:1 with sidekiq-dags DummyJob).
class DummyJob
  include Kiq::Job

  def perform(desc : ::Kiq::Arg = "dummy")
    # empty coordination job
  end
end

module Kiq
  class Node
    include Kiq::Job
    include Kiq::Batch::Callback::CallbackHandler
    include Kiq::Batch::Callback::OnComplete

    # Compile-time registry: "pipeline_name/node_name" -> node class,
    # used by PipelineCallback to trigger the next node.
    REGISTRY = {} of String => ::Kiq::Node.class

    macro inherited
      {% ns = @type.stringify.split("::")[0..-2].join("::") %}
      {% leaf = @type.stringify.split("::").last %}
      {% full_ns = ns == "" ? @type.stringify : ns %}
      ::Kiq::Node::REGISTRY[::Kiq::Pipeline.underscore({{ full_ns }}) + "/" + {{ leaf }}] = {{ @type.id }}

      # subclasses are batch callbacks too (macro included does not fire
      # for them)
      ::Kiq::Batch::Callback::Registry.register({{ @type.stringify }}, {{ @type.id }}.new)

      {% ::Kiq::JOBS << @type %}

      # The class-level enqueue API baked into Kiq::Job at include time
      # carries Kiq::Node's name; redefine with this subclass's name.
      def self.perform_async : String? | Bool
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_async
      end

      def self.perform_async(*args : ::Kiq::Arg) : String? | Bool
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_async(*args)
      end

      def self.perform_in(interval : Float64 | Int32 | ::Time::Span | ::Time, *args : ::Kiq::Arg) : String?
        ::Kiq::Job::Setter.new({{ @type.stringify }}).perform_in(interval, *args)
      end

      def self.set(**opts) : ::Kiq::Job::Setter
        ::Kiq::Job::Setter.new({{ @type.stringify }}).set(**opts)
      end
    end

    macro desc(str)
      def desc : String
        {{ str }}
      end
    end

    macro execute(&block)
      def execute(args : Array(::Kiq::Arg)) : Nil
        {{ block.body }}
      end
    end

    macro next_node(node)
      def next_node : ::Kiq::Node.class | Nil
        {% if node.is_a?(SymbolLiteral) || node.is_a?(StringLiteral) %}
          {% parts = node.id.stringify.split("_") %}
          {% camel = "" %}
          {% for part in parts %}
            {% head = part.size > 0 ? part[0...1].upcase : "" %}
            {% tail = part.size > 1 ? part[1..-1] : "" %}
            {% camel = camel + head + tail %}
          {% end %}
          {{ camel.id }}
        {% elsif node.is_a?(Path) %}
          {{ node.id }}
        {% else %}
          nil
        {% end %}
      end
    end

    def desc : String
      "Kiq::Node"
    end

    def execute(args : Array(::Kiq::Arg)) : Nil
    end

    def next_node : ::Kiq::Node.class | Nil
      nil
    end

    def observer : Nil
    end

    def custom_notifiers(prefix : String, msg : String) : Nil
    end

    # Pipeline tracking hook — overridden by Kiq::PipelineTracking.
    def mark_node_started! : Bool
      true
    end

    # Registered when the node class tracks pipeline status.
    def pipeline_tracking? : Bool
      false
    end

    def pipeline_name : String
      Pipeline.underscore(self.class.name.to_s.split("::")[0..-2].join("::"))
    end

    def node_name : String
      self.class.name.to_s.split("::").last
    end

    def perform(*args) : Nil
      job_args = args.to_a.map(&.as(::Kiq::Arg))
      options = {} of String => Arg
      case job_args.last?
      when Hash(String, Arg) then options = job_args.pop.as(Hash(String, Arg))
      end
      single_mode = single?(options["single"]?)
      observer

      return unless mark_node_started!

      batch = Batch.new
      @batch = batch

      save_bid_to_database!(batch.bid)

      batch.add_jobs do
        # execute FIRST so all real jobs register before DummyJob (1:1).
        execute(job_args)
        DummyJob.perform_async(desc)
      end

      if pipeline_tracking?
        batch.on(:complete, Batch::Callback::PipelineCallback, {
          "pipeline_name" => pipeline_name,
          "node_name"     => node_name,
          "single"        => single_mode,
        } of String => Arg)
      else
        batch.on(:complete, self.class, {"single" => single_mode} of String => Arg)
      end
      batch.run

      s = Batch::ExplicitStatus.new(batch.bid)
      notify_all "➡️ #{desc} -> (#{s.total})    | #{batch.bid}"
    end

    def on_complete(status : Kiq::Batch::FinalStatusSnapshot, options : Hash(String, Arg)) : Nil
      single_mode = single?(options["single"]?)

      notify_all "✔️ #{desc} (#{status.total})   |  #{status.bid}"

      if single_mode
        notify_all "🔶 Single mode - skipping next_node"
        return
      end

      if (next_node_class = next_node)
        notify_all "➕ #{next_node_class}"
        next_node_class.perform_async
      else
        notify_all "✅ Конец графа #{kiq_queue}."
      end
    end

    def save_bid_to_database!(bid : String?) : Nil
      return unless bid

      if pipeline_tracking?
        PipelineStatus.save_bid!(pipeline_name, node_name, bid)
      end
    rescue ex : Exception
      # best effort, like the original
    end

    def notify_all(msg : String) : Nil
      custom_notifiers("[#{kiq_queue}] ", msg)
    end

    def kiq_queue : String
      Kiq.job_options(self.class.name.to_s)["queue"].to_s
    end

    def batch : Batch?
      @batch
    end

    @batch : Batch?

    private def single?(v : Arg) : Bool
      case v
      when Bool then v
      when String then v == "true"
      else false
      end
    end
  end

  # Pipeline status tracking for node classes — include into a base node
  # class (1:1 with Sidekiq::PipelineTracking).
  module PipelineTracking
    macro included
      @current_pipeline : ::Kiq::Pipeline?
      @current_node_record : ::Kiq::PipelineNode?

      def pipeline_tracking? : Bool
        true
      end

      def mark_node_started! : Bool
        if node_name == "RootNode"
          if current_pipeline.running?
            return false
          end
          current_pipeline.start!
        end

        if (b = batch) && b.bid
          current_node_record.bid = b.bid
        end
        current_node_record.start!
        true
      rescue ex : Exception
        false
      end

      def current_pipeline : ::Kiq::Pipeline
        @current_pipeline ||= ::Kiq::Pipeline.for(pipeline_name)
      end

      def current_node_record : ::Kiq::PipelineNode
        @current_node_record ||= ::Kiq::PipelineNode.for(pipeline_name, node_name)
      end
    end
  end

  # Completes tracked nodes and triggers the next one (1:1 with
  # Sidekiq::PipelineCallback; node classes resolve through the
  # compile-time registry instead of const_get).
  class Batch
    module Callback
      class PipelineCallback
        include ::Kiq::Batch::Callback::CallbackHandler
        include ::Kiq::Batch::Callback::OnComplete

        def on_complete(status : ::Kiq::Batch::FinalStatusSnapshot, options : Hash(String, Arg)) : Nil
          handle_event(status, options, "complete")
        end

        private def handle_event(status, options, event_type) : Nil
          pipeline_name = options["pipeline_name"]?.try(&.to_s)
          node_name = options["node_name"]?.try(&.to_s)
          return unless pipeline_name && node_name

          node_record = PipelineNode.for(pipeline_name, node_name)
          pipeline = Pipeline.for(pipeline_name)

          # Verify the batch is actually finished (pending == failures or 0).
          Batch.with_redis_lock("batch-lock-#{status.bid}", timeout: 5) do
            pending_str = Kiq.default_configuration.redis_client
              .hget("BID-#{status.bid}", "pending").try(&.to_s)
            # pending nil => batch gone; if pending > failures the batch is
            # still working — ignore the premature callback (1:1 check).
            if (p = pending_str.try(&.to_i64?)) && p > status.failures
              return
            end
          end

          case event_type
          when "complete"
            node_record.start! if node_record.pending?
            return unless node_record.running?
            return if node_record.completed? || node_record.failed?

            node_record.complete!

            single_mode = options["single"]?.try(&.to_s) == "true" || options["single"]? == true

            if single_mode
              # skip next node
            elsif node_name == "EndNode"
              pipeline.finish!(success: true)
            else
              trigger_next_node(pipeline_name, node_name)
            end
          end
        end

        def trigger_next_node(pipeline_name : String, node_name : String) : Nil
          if (current = Node::REGISTRY["#{pipeline_name}/#{node_name}"]?)
            instance = current.new
            if (next_node_class = instance.next_node)
              next_node_class.perform_async
            end
          end
        end
      end
    end
  end

  # Pipeline DSL (macro-based port of Sidekiq::NodeDsl#pipeline).
  module NodeDsl
    macro pipeline(name, &block)
      {% nparts = name.id.stringify.split("_") %}
        {% ncamel = "" %}
        {% for part in nparts %}
          {% head = part.size > 0 ? part[0...1].upcase : "" %}
          {% tail = part.size > 1 ? part[1..-1] : "" %}
          {% ncamel = ncamel + head + tail %}
        {% end %}
        module {{ ncamel.id }}
        {% if block %}
          {% body = block.body %}
          {% exprs = body.is_a?(Expressions) ? body.expressions : [body] %}
          {% for expr in exprs %}
            {% if expr.is_a?(Call) && expr.name.stringify == "node" %}
              ::Kiq::NodeDsl.node {{ expr.args[0] }} {{ expr.block }}
            {% end %}
          {% end %}
        {% end %}
      end
    end

    macro node(name, &block)
      {% cparts = name.id.stringify.split("_") %}
        {% ccamel = "" %}
        {% for part in cparts %}
          {% head = part.size > 0 ? part[0...1].upcase : "" %}
          {% tail = part.size > 1 ? part[1..-1] : "" %}
          {% ccamel = ccamel + head + tail %}
        {% end %}
        class {{ ccamel.id }} < ::Kiq::Node
        include ::Kiq::PipelineTracking

        {% if block %}
          {% body = block.body %}
          {% exprs = body.is_a?(Expressions) ? body.expressions : [body] %}
          {% for expr in exprs %}
            {% if expr.is_a?(Call) %}
              {% if expr.name.stringify == "desc" %}
                desc {{ expr.args[0] }}
              {% elsif expr.name.stringify == "execute" %}
                execute {{ expr.block }}
              {% elsif expr.name.stringify == "next_node" %}
                next_node {{ expr.args[0] }}
              {% end %}
            {% end %}
          {% end %}
        {% end %}
      end
    end
  end
end

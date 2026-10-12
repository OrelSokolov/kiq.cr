# Kiq DAG pipelines — 1:1 port of sidekiq-dags (Sidekiq::Node, NodeDsl,
# PipelineStatus, SidekiqPipeline, SidekiqPipelineNode, PipelineCallback,
# PipelineTracking, DummyJob).
#
# Source: oracle/vendor/sidekiq-dags/lib/sidekiq/{node,node_dsl,
# pipeline_status,pipeline,pipeline_node,pipeline_callback,
# pipeline_tracking,dummy_job}.rb
#
# Deviations (Crystal port):
# - Node DSL is macro-based; `next_node :other_node` resolves the sibling
#   class constant at compile time instead of const_get at runtime (which
#   is broken in the original for namespaced pipelines — see PLAN.md).
# - Node/callback class resolution goes through a compile-time registry.

module Kiq
  # Redis-backed pipeline/node status store — 1:1 with Sidekiq::PipelineStatus.
  class PipelineStatus
    def self.running?(pipeline_name : String) : Bool
      redis.get("pipeline:#{pipeline_name}:status").try(&.to_s) == "running"
    end

    def self.start!(pipeline_name : String) : Nil
      redis.multi do |tx|
        tx.set("pipeline:#{pipeline_name}:status", "running")
        tx.set("pipeline:#{pipeline_name}:run_at", Time.utc.to_s("%Y-%m-%dT%H:%M:%SZ"))
      end
    end

    def self.finish!(pipeline_name : String, success : Bool = true) : Nil
      redis.set("pipeline:#{pipeline_name}:status", success ? "completed" : "failed")
    end

    def self.reset!(pipeline_name : String) : Nil
      keys = array_of_strings(redis.keys("pipeline:#{pipeline_name}:nodes:*"))
      redis.del(keys) unless keys.empty?
      redis.del("pipeline:#{pipeline_name}:status")
    end

    def self.node_status(pipeline_name : String, node_name : String) : String
      redis.get("pipeline:#{pipeline_name}:nodes:#{node_name}:status").try(&.to_s) || "pending"
    end

    def self.node_start!(pipeline_name : String, node_name : String, bid : String? = nil) : Nil
      redis.multi do |tx|
        tx.set("pipeline:#{pipeline_name}:nodes:#{node_name}:status", "running")
        tx.set("pipeline:#{pipeline_name}:nodes:#{node_name}:run_at", Time.utc.to_s("%Y-%m-%dT%H:%M:%SZ"))
        if bid
          tx.set("pipeline:#{pipeline_name}:nodes:#{node_name}:bid", bid)
          tx.set("pipeline:bid:#{bid}", "#{pipeline_name}:#{node_name}")
        end
      end
    end

    def self.node_complete!(pipeline_name : String, node_name : String) : Nil
      redis.set("pipeline:#{pipeline_name}:nodes:#{node_name}:status", "completed")
    end

    def self.node_fail!(pipeline_name : String, node_name : String, error : String? = nil) : Nil
      redis.multi do |tx|
        tx.set("pipeline:#{pipeline_name}:nodes:#{node_name}:status", "failed")
        tx.set("pipeline:#{pipeline_name}:nodes:#{node_name}:error", error.to_s) if error
      end
    end

    def self.node_bid(pipeline_name : String, node_name : String) : String?
      redis.get("pipeline:#{pipeline_name}:nodes:#{node_name}:bid").try(&.to_s)
    end

    def self.save_bid!(pipeline_name : String, node_name : String, bid : String?) : Nil
      return unless bid
      redis.multi do |tx|
        tx.set("pipeline:#{pipeline_name}:nodes:#{node_name}:bid", bid)
        tx.set("pipeline:bid:#{bid}", "#{pipeline_name}:#{node_name}")
      end
    end

    def self.all_node_statuses(pipeline_name : String) : Hash(String, String)
      keys = array_of_strings(redis.keys("pipeline:#{pipeline_name}:nodes:*:status"))
      return {} of String => String if keys.empty?

      values = keys.map { |k| redis.get(k).try(&.to_s) }
      result = {} of String => String
      keys.each_with_index do |key, index|
        if (match = key.match(/nodes:([^:]+):status/))
          result[match[1]] = values[index].to_s
        end
      end
      result
    end

    def self.running_node_ids(pipeline_name : String) : Array(String)
      all_node_statuses(pipeline_name).select { |_, status| status == "running" }.keys
    end

    def self.completed_nodes_count(pipeline_name : String) : Int32
      all_node_statuses(pipeline_name).count { |_, status| {"completed", "skipped"}.includes?(status) }
    end

    def self.node_progress(pipeline_name : String, node_name : String) : Hash(String, Arg)?
      data = redis.hgetall("pipeline:#{pipeline_name}:nodes:#{node_name}:progress")
      h = hash_of_strings(data)
      return nil if h.empty?

      {
        "bid"                => h["bid"]?,
        "max"                => h["total"]?.try(&.to_i64?) || 0_i64,
        "pending"            => h["pending"]?.try(&.to_i64?) || 0_i64,
        "done"               => h["done"]?.try(&.to_i64?) || 0_i64,
        "progress_percentage" => h["progress_percentage"]?.try(&.to_f64?) || 0.0,
      }
    end

    def self.find_node_by_bid(bid : String) : {String, String}? | Hash(String, String) | Nil
      return nil unless bid
      data = redis.get("pipeline:bid:#{bid}").try(&.to_s)
      return nil unless data

      parts = data.split(":", 2)
      {"pipeline_name" => parts[0], "node_id" => parts[1]}
    end

    def self.redis
      Kiq.default_configuration.redis_client
    end

    def self.array_of_strings(v) : Array(String)
      case v
      when Array then v.map(&.to_s)
      else [] of String
      end
    end

    def self.hash_of_strings(v) : Hash(String, String)
      h = {} of String => String
      case v
      when Hash then v.each { |k, x| h[k.to_s] = x.to_s }
      when Array
        # this redis client returns HGETALL as a flat [k1, v1, k2, v2] array
        arr = v.map(&.to_s)
        (0...(arr.size - 1)).step(2) do |i|
          h[arr[i]] = arr[i + 1]
        end
      end
      h
    end
  end

  # Pipeline record over PipelineStatus — 1:1 with Sidekiq::SidekiqPipeline.
  class Pipeline
    getter pipeline_name : String

    def self.for(name : String) : Pipeline
      new(underscore(name.to_s))
    end

    def self.underscore(string : String) : String
      string
        .gsub("::", "/")
        .gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
        .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
        .tr("-", "_")
        .downcase
    end

    def initialize(@pipeline_name)
    end

    def start! : Nil
      PipelineStatus.start!(@pipeline_name)
    end

    def finish!(success : Bool = true) : Nil
      PipelineStatus.finish!(@pipeline_name, success: success)
    end

    def running? : Bool
      return true if PipelineStatus.running_node_ids(@pipeline_name).any?
      has_jobs_in_queue?
    end

    def completed? : Bool
      node_ids = all_node_ids
      return false if node_ids.empty?

      statuses = PipelineStatus.all_node_statuses(@pipeline_name)
      return false if statuses.empty?

      node_ids.all? do |node_id|
        status = statuses[node_id]?
        status == "completed" || status == "skipped"
      end
    end

    def failed? : Bool
      PipelineStatus.all_node_statuses(@pipeline_name).values.includes?("failed")
    end

    def idle? : Bool
      !running?
    end

    def status : String
      return "failed" if failed?
      return "running" if running?
      return "completed" if completed?
      "idle"
    end

    def current_node : PipelineNode?
      running_nodes = PipelineStatus.running_node_ids(@pipeline_name)
      return nil if running_nodes.empty?

      PipelineNode.new(@pipeline_name, running_nodes.first)
    end

    def completed_nodes : Array(PipelineNode)
      PipelineStatus.all_node_statuses(@pipeline_name)
        .select { |_, status| status == "completed" || status == "skipped" }
        .map { |node_id, _| PipelineNode.new(@pipeline_name, node_id) }
    end

    def sidekiq_pipeline_nodes : Array(PipelineNode)
      all_node_ids.map { |node_id| PipelineNode.new(@pipeline_name, node_id) }
    end

    def progress_percent : Float64
      total = all_node_ids.size
      return 0.0 if total.zero?

      completed = PipelineStatus.completed_nodes_count(@pipeline_name)
      ((completed.to_f / total) * 100).round(1)
    end

    def update!(attrs : Hash(String, Arg)) : Nil
      if run_at = attrs["run_at"]?
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:run_at", run_at.to_s)
      end
      if st = attrs["status"]?
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:status", st.to_s)
      end
    end

    private def all_node_ids : Array(String)
      keys = PipelineStatus.array_of_strings(PipelineStatus.redis.keys("pipeline:#{@pipeline_name}:nodes:*:status"))
      keys.map { |k| k.match(/nodes:([^:]+):status/).try(&.[1]).to_s }.uniq
    end

    private def has_jobs_in_queue? : Bool
      queue = Kiq::Queue.new(@pipeline_name)
      return true if queue.size > 0
      false
    end
  end

  # Node record over PipelineStatus — 1:1 with Sidekiq::SidekiqPipelineNode.
  class PipelineNode
    getter pipeline_name : String
    getter node_name : String

    def self.for(pipeline_name : String, node_name : String) : PipelineNode
      new(Pipeline.underscore(pipeline_name), node_name.to_s)
    end

    def initialize(@pipeline_name, @node_name)
    end

    def start! : Nil
      PipelineStatus.node_start!(@pipeline_name, @node_name, bid: @bid)
    end

    def complete! : Nil
      PipelineStatus.node_complete!(@pipeline_name, @node_name)
    end

    def fail!(error : String? = nil) : Nil
      PipelineStatus.node_fail!(@pipeline_name, @node_name, error)
    end

    def skip! : Nil
      # no status transition in the original
    end

    def full_name : String
      "#{@pipeline_name}::#{@node_name}"
    end

    def node_id : String
      "#{@pipeline_name}_#{@node_name}"
    end

    def status : String
      PipelineStatus.node_status(@pipeline_name, @node_name)
    end

    def bid : String?
      @bid ||= PipelineStatus.node_bid(@pipeline_name, @node_name)
    end

    def bid=(value : String) : Nil
      @bid = value
      PipelineStatus.save_bid!(@pipeline_name, @node_name, value) if value
    end

    def error_message : String?
      PipelineStatus.redis.get("pipeline:#{@pipeline_name}:nodes:#{@node_name}:error").try(&.to_s)
    end

    def pending? : Bool
      status == "pending"
    end

    def running? : Bool
      status == "running"
    end

    def completed? : Bool
      status == "completed"
    end

    def failed? : Bool
      status == "failed"
    end

    def skipped? : Bool
      status == "skipped"
    end

    def reload : self
      @bid = nil
      self
    end

    def sidekiq_pipeline : Pipeline
      Pipeline.for(@pipeline_name)
    end
  end
end

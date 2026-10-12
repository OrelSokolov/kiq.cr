# frozen_string_literal: true

module Sidekiq
  # Класс для управления состоянием нод пайплайнов в Redis
  class SidekiqPipelineNode
    attr_reader :pipeline_name, :node_name

    # Clear all node data from Redis (for test cleanup)
    def self.destroy_all
      # Nodes are cleared when pipeline.destroy_all is called
      # This method exists for API compatibility with ActiveRecord
    end

    def self.for(pipeline_name, node_name)
      pipeline = SidekiqPipeline.for(pipeline_name)
      return nil unless pipeline

      node = new(pipeline_name, node_name.to_s)
      node
    rescue => e
      Sidekiq.logger.error "Error creating node #{pipeline_name}::#{node_name}: #{e.message}" if Sidekiq.logger
      nil
    end

    def initialize(pipeline_name, node_name)
      @pipeline_name = pipeline_name.to_s
      @node_name = node_name.to_s
    end

    def start!
      puts "[SET STATUS]"
      PipelineStatus.node_start!(@pipeline_name, @node_name, bid: @bid)
      Sidekiq.logger.info "▶️ Node #{full_name} started" if Sidekiq.logger
    end

    def complete!
      puts "[SET STATUS]"
      PipelineStatus.node_complete!(@pipeline_name, @node_name)
      Sidekiq.logger.info "✅ Node #{full_name} completed" if Sidekiq.logger
    end

    def fail!(error = nil)
      puts "[SET STATUS]"
      PipelineStatus.node_fail!(@pipeline_name, @node_name, error)
      Sidekiq.logger.info "❌ Node #{full_name} failed: #{error}" if Sidekiq.logger
    end

    def skip!
      puts "[SET STATUS]"
      Sidekiq.logger.info "⏭️ Node #{full_name} skipped" if Sidekiq.logger
    end

    def full_name
      "#{@pipeline_name}::#{@node_name}"
    end

    def node_id
      "#{@pipeline_name}_#{@node_name}"
    end

    def status
      PipelineStatus.node_status(@pipeline_name, @node_name)
    end

    def run_at
      time_str = PipelineStatus.redis.get("pipeline:#{@pipeline_name}:nodes:#{@node_name}:run_at")
      Time.parse(time_str) if time_str
    end

    def bid
      @bid ||= PipelineStatus.node_bid(@pipeline_name, @node_name)
    end

    def bid=(value)
      @bid = value
      PipelineStatus.save_bid!(@pipeline_name, @node_name, value) if value
    end

    def error_message
      PipelineStatus.redis.get("pipeline:#{@pipeline_name}:nodes:#{@node_name}:error")
    end

    def pending?
      status == 'pending'
    end

    def running?
      status == 'running'
    end

    def completed?
      status == 'completed'
    end

    def failed?
      status == 'failed'
    end

    def skipped?
      status == 'skipped'
    end

    def update!(attrs)
      if attrs[:bid]
        self.bid = attrs[:bid]
      end
      if attrs[:status]
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:nodes:#{@node_name}:status", attrs[:status].to_s)
      end
      if attrs[:run_at]
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:nodes:#{@node_name}:run_at", attrs[:run_at].iso8601)
      end
      if attrs[:error_message]
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:nodes:#{@node_name}:error", attrs[:error_message].to_s)
      end
    end

    def update_column(column, value)
      case column
      when :bid
        self.bid = value
      when :status
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:nodes:#{@node_name}:status", value.to_s)
      when :run_at
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:nodes:#{@node_name}:run_at", value.to_s)
      when :error_message
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:nodes:#{@node_name}:error", value.to_s)
      end
    end

    def reload
      @bid = nil
      self
    end

    def sidekiq_pipeline
      SidekiqPipeline.for(@pipeline_name)
    end

    # Query methods для совместимости
    def self.where(conditions = {})
      SidekiqPipelineNodeQuery.new(conditions)
    end
  end

  # Mock query object
  class SidekiqPipelineNodeQuery
    def initialize(conditions = {})
      @conditions = conditions
      @pipeline = conditions[:sidekiq_pipeline]
      @pipeline_name = @pipeline&.pipeline_name
    end

    def running
      return [] unless @pipeline_name
      
      node_ids = PipelineStatus.running_node_ids(@pipeline_name)
      node_ids.map { |id| SidekiqPipelineNode.new(@pipeline_name, id) }
    end

    def completed
      return [] unless @pipeline_name
      
      statuses = PipelineStatus.all_node_statuses(@pipeline_name)
      statuses
        .select { |_, status| status == 'completed' || status == 'skipped' }
        .map { |id, _| SidekiqPipelineNode.new(@pipeline_name, id) }
    end

    def where(conditions = {})
      @conditions.merge!(conditions)
      self
    end

    def exists?
      count > 0
    end

    def count
      all.count
    end

    def first
      all.first
    end

    def each(&block)
      all.each(&block)
    end

    def map(&block)
      all.map(&block)
    end

    def update_all(attrs)
      all.each do |node|
        node.update!(attrs)
      end
      all.count
    end

    private

    def all
      return [] unless @pipeline_name
      
      statuses = PipelineStatus.all_node_statuses(@pipeline_name)
      
      if @conditions[:status]
        target_statuses = Array(@conditions[:status]).map(&:to_s)
        statuses = statuses.select { |_, s| target_statuses.include?(s) }
      end
      
      statuses.map { |id, _| SidekiqPipelineNode.new(@pipeline_name, id) }
    end
  end
end

require_relative 'pipeline_status'

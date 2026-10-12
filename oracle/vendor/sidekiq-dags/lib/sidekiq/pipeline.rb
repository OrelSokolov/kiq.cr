# frozen_string_literal: true

module Sidekiq
  # Класс для управления состоянием пайплайнов в Redis
  class SidekiqPipeline
    attr_reader :pipeline_name

    @test_queues_with_jobs = Set.new

    class << self
      attr_accessor :test_queues_with_jobs

      # Clear all pipeline data from Redis (for test cleanup)
      def destroy_all
        Sidekiq.redis do |conn|
          keys = conn.keys("pipeline:*")
          conn.del(*keys) if keys.any?
        end
      end

      def for(name)
        pipeline_name = underscore(name.to_s)
        puts "Create pipeline for #{name}: #{pipeline_name}".colorize(:red) if defined?(Colorize)
        
        new(pipeline_name)
      rescue => e
        Sidekiq.logger.error "Error creating pipeline #{name}: #{e.message}" if Sidekiq.logger
        nil
      end

      def underscore(string)
        string.to_s.gsub(/::/, '/')
              .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
              .gsub(/([a-z\d])([A-Z])/, '\1_\2')
              .tr('-', '_')
              .downcase
      end
    end

    def initialize(pipeline_name)
      @pipeline_name = pipeline_name
    end

    def start!
      PipelineStatus.start!(@pipeline_name)
      Sidekiq.logger.info "🚀 Pipeline #{pipeline_name} started" if Sidekiq.logger
    end

    def finish!(success: true, error: nil)
      new_status = success ? 'completed' : 'failed'
      PipelineStatus.finish!(@pipeline_name, success: success)
      Sidekiq.logger.info "#{success ? '✅' : '❌'} Pipeline #{pipeline_name} finished with status: #{new_status}" if Sidekiq.logger
    end

    def running?
      return true if PipelineStatus.running_node_ids(@pipeline_name).any?
      return true if has_jobs_in_queue?
      false
    end

    def completed?
      node_ids = all_node_ids
      return false if node_ids.empty?
      
      statuses = PipelineStatus.all_node_statuses(@pipeline_name)
      return false if statuses.empty?
      
      node_ids.all? do |node_id|
        status = statuses[node_id]
        status == 'completed' || status == 'skipped'
      end
    end

    def failed?
      PipelineStatus.all_node_statuses(@pipeline_name).values.include?('failed')
    end

    def idle?
      !running?
    end

    def active?
      running?
    end

    def status
      return 'failed' if failed?
      return 'running' if running?
      return 'completed' if completed?
      'idle'
    end

    def current_node
      running_nodes = PipelineStatus.running_node_ids(@pipeline_name)
      return nil if running_nodes.empty?
      
      node_id = running_nodes.first
      SidekiqPipelineNode.new(@pipeline_name, node_id)
    end

    def completed_nodes
      statuses = PipelineStatus.all_node_statuses(@pipeline_name)
      statuses
        .select { |_, status| status == 'completed' || status == 'skipped' }
        .map { |node_id, _| SidekiqPipelineNode.new(@pipeline_name, node_id) }
    end

    def sidekiq_pipeline_nodes
      all_node_ids.map { |node_id| SidekiqPipelineNode.new(@pipeline_name, node_id) }
    end

    def progress_percent
      total = all_node_ids.count
      return 0 if total.zero?

      completed = PipelineStatus.completed_nodes_count(@pipeline_name)
      ((completed.to_f / total) * 100).round(1)
    end

    def update!(attrs)
      if attrs[:run_at]
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:run_at", attrs[:run_at].iso8601)
      end
      if attrs[:status]
        PipelineStatus.redis.set("pipeline:#{@pipeline_name}:status", attrs[:status].to_s)
      end
    end

    private

    def all_node_ids
      keys = PipelineStatus.redis.keys("pipeline:#{@pipeline_name}:nodes:*:status")
      keys.map { |k| k.match(/nodes:([^:]+):status/)&.[](1) }.compact.uniq
    end

    def queue_name
      @pipeline_name
    end

    def has_jobs_in_queue?
      return false if queue_name.blank?

      if defined?(Rails) && Rails.env.test? && self.class.test_queues_with_jobs&.include?(queue_name)
        return true
      end

      begin
        queue = Sidekiq::Queue.new(queue_name)
        return true if queue.size > 0

        workers = Sidekiq::Workers.new
        workers.each do |_process_id, _thread_id, work|
          return true if work['queue'] == queue_name
        end

        false
      rescue StandardError => e
        Sidekiq.logger.debug "Queue #{queue_name} check failed: #{e.message}" if Sidekiq.logger
        false
      end
    end
  end
end

require_relative 'pipeline_status'

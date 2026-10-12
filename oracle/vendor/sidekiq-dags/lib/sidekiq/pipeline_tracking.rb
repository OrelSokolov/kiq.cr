# frozen_string_literal: true

module Sidekiq
  # Concern для отслеживания статусов пайплайна в Redis
  module PipelineTracking
    extend ::ActiveSupport::Concern

    included do
    end

    def pipeline_name
      class_name = self.class.name
      if class_name.include?('::')
        namespace = class_name.split('::')[0..-2].join('::')
        underscore(namespace)
      else
        underscore(class_name)
      end
    end

    def underscore(string)
      string.to_s.gsub(/::/, '/')
            .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
            .gsub(/([a-z\d])([A-Z])/, '\1_\2')
            .tr('-', '_')
            .downcase
    end

    def node_name
      self.class.name.split('::').last
    end

    def pipeline_running?
      SidekiqPipeline.for(pipeline_name).running?
    end

    def current_pipeline
      @current_pipeline ||= SidekiqPipeline.for(pipeline_name)
    end

    def current_node_record
      @current_node_record ||= SidekiqPipelineNode.for(pipeline_name, node_name)
    end

    def ensure_node_record_exists!
      current_node_record
    end

    def mark_node_started!
      return false unless current_pipeline && current_node_record

      ensure_node_record_exists!

      if node_name == 'RootNode'
        if current_pipeline.running?
          Sidekiq.logger.warn "🛑 Pipeline #{pipeline_name} already running, skipping start"
          return false
        end
        current_pipeline.start!
      end

      if @batch && @batch.bid
        current_node_record.bid = @batch.bid
      end

      current_node_record.start!
      broadcast_status_change(pipeline_name, node_name, 'running')
      true
    rescue => e
      Sidekiq.logger.debug "Pipeline tracking error: #{e.message}" if Sidekiq.logger
      false
    end

    def mark_node_completed!
      puts "CURRENT NODE RECORD: #{current_node_record.inspect}"
      return unless current_node_record

      ensure_node_record_exists!
      current_node_record.complete!
      broadcast_status_change(pipeline_name, node_name, 'completed')

      current_pipeline.finish!(success: true) if node_name == 'EndNode' && current_pipeline
    rescue => e
      Sidekiq.logger.debug "Pipeline tracking error: #{e.message}" if Sidekiq.logger
    end

    def mark_node_failed!(error)
      return unless current_node_record && current_pipeline

      ensure_node_record_exists!
      current_node_record.fail!(error.message)
      broadcast_status_change(pipeline_name, node_name, 'failed')
      current_pipeline.finish!(success: false, error: error.message)
    rescue => e
      Sidekiq.logger.debug "Pipeline tracking error: #{e.message}" if Sidekiq.logger
    end

    def self.get_active_nodes_for_ui(pipeline_name = nil)
      if pipeline_name
        statuses = PipelineStatus.all_node_statuses(pipeline_name)
        statuses
          .select { |_, status| %w[running completed].include?(status) }
          .map { |node_id, _| "#{pipeline_name}_#{node_id}" }
      else
        # Получаем все pipeline names из Redis
        keys = Sidekiq.redis { |conn| conn.keys("pipeline:*:status") }
        pipeline_names = keys.map { |k| k.match(/pipeline:([^:]+):status/)&.[](1) }.compact
        
        pipeline_names.flat_map do |name|
          statuses = PipelineStatus.all_node_statuses(name)
          statuses
            .select { |_, status| %w[running completed].include?(status) }
            .map { |node_id, _| "#{name}_#{node_id}" }
        end
      end
    end

    private

    # Broadcast статуса ноды через ActionCable (если доступно)
    def broadcast_status_change(pipeline_name, node_name, status)
      # Проверяем наличие PipelineBroadcastService в Rails приложении
      if defined?(PipelineBroadcastService)
        begin
          PipelineBroadcastService.broadcast_node_progress(pipeline_name, node_name, status, nil)
          Sidekiq.logger.debug "📡 Broadcasted status change for #{pipeline_name}/#{node_name}: #{status}" if Sidekiq.logger
        rescue => e
          Sidekiq.logger.debug "Failed to broadcast status change: #{e.message}" if Sidekiq.logger
        end
      end
    end
  end
end

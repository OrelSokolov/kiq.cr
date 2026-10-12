require 'sidekiq/batch'
require 'colorize'

module Sidekiq
  class Node
    include Sidekiq::Worker
    include Sidekiq::Batch::Callback

    def self.supports_single_mode
      true
    end

    def self.execute(&block)
      define_method(:execute) do |*args, **kwargs|
        # Проверяем arity блока чтобы решить как вызывать
        # arity = 0 означает блок не принимает аргументы
        # arity < 0 означает блок принимает переменное число аргументов (*args)
        if block.arity == 0
          # Блок не принимает аргументы - вызываем без них
          instance_exec(&block)
        else
          # Блок принимает аргументы или использует *args
          instance_exec(*args, **kwargs, &block)
        end
      end
    end

    def self.desc(str)
      define_method(:desc) do
        str
      end
    end

    def self.next_node(arg)
      define_method(:next_node) do
        klass = arg
        if klass.is_a?(Symbol)
          class_name = self.class.name
          # Извлекаем namespace из имени класса
          namespace = if class_name.include?('::')
                        class_name.split('::')[0..-2].join('::')
                      else
                        ''
                      end
          full_name = namespace.empty? ? klass.to_s : "#{namespace}::#{klass}"
          # Используем constantize из ActiveSupport или простой поиск константы
          if defined?(ActiveSupport::Inflector)
            full_name.constantize
          else
            # Простой поиск константы
            full_name.split('::').inject(Object) { |o, name| o.const_get(name) }
          end
        else
          klass
        end
      end
    end

    def self.observer(&block)
      define_method(:observer) do
        instance_exec(&block)
      end
    end

    def desc
      'Sidekiq::Node'
    end

    def execute(*args, **kwargs); end

    def next_node
      nil
    end

    def observer; end

    def custom_notifiers(prefix, msg); end

    def perform(*args, **kwargs)
      # Sidekiq serializes kwargs to JSON, so single may be in args (last element if it's a Hash)
      options = args.last.is_a?(Hash) ? args.pop : {}
      single_mode = kwargs.delete(:single) || kwargs.delete('single') ||
                    options['single'] || options[:single] || false
      observer

      # Отслеживание начала ноды (если включен PipelineTracking)
      if respond_to?(:mark_node_started!)
        started = mark_node_started!
        return unless started # Если пайплайн уже запущен, не запускаем ноду
      end

      @batch = Sidekiq::Batch.new

      # Сохраняем BID в базу данных для отслеживания прогресса
      save_bid_to_database!(@batch.bid)

      @batch.add_jobs do
        # ВАЖНО: execute ПЕРВЫМ, чтобы все реальные джобы были зарегистрированы
        # до DummyJob. Иначе DummyJob может завершиться раньше, чем реальные джобы
        # будут добавлены в batch, вызывая race condition.
        execute(*args, **kwargs)
        DummyJob.perform_async(desc) # Нужен для гарантии непустого batch
      end

      # Регистрируем callback для отслеживания статусов пайплайна
      # Если используется PipelineTracking, PipelineCallback будет запускать следующую ноду
      # Стандартный callback нужен только для логирования
      if respond_to?(:pipeline_name) && respond_to?(:node_name)
        pipeline_name = self.pipeline_name
        node_name = self.node_name
        Sidekiq.logger.info "🔔 Registering PipelineCallback for #{pipeline_name}::#{node_name} (bid: #{@batch.bid})"
        @batch.on(:complete, Sidekiq::PipelineCallback, {
                    'pipeline_name' => pipeline_name,
                    'node_name' => node_name,
                    'single' => single_mode
                  })
        @batch.on(:failure, Sidekiq::PipelineCallback, {
                    'pipeline_name' => pipeline_name,
                    'node_name' => node_name,
                    'single' => single_mode
                  })
        # Не регистрируем стандартный callback, если используется PipelineTracking
        # PipelineCallback сам запустит следующую ноду
      else
        # Если PipelineTracking не используется, используем стандартный callback
        Sidekiq.logger.info "🔔 Registering standard callback for #{self.class} (no PipelineTracking)"
        @batch.on(:complete, self.class, 'single' => single_mode)
      end
      @batch.run

      s = Sidekiq::Batch::ExplicitStatus.new(@batch.bid)
      # Sidekiq.logger.info "#{Time.current.to_f} 🔥 EXISTS? #{s.exists?}".colorize(:red)
      # Sidekiq.logger.info s.data.to_s.colorize(:light_yellow)

      desc_str = desc
      desc_str = nil if desc_str.respond_to?(:present?) && !desc_str.present?
      desc_str = desc if desc_str.nil? || (desc_str.respond_to?(:empty?) && desc_str.empty?)
      notify_all "➡️ #{desc_str || self.class} -> (#{s.total})    | #{@batch.bid}"
    end

    def on_complete(status, options)
      single_mode = options['single'] || options[:single]

      desc_str = desc
      desc_str = nil if desc_str.respond_to?(:present?) && !desc_str.present?
      desc_str = desc if desc_str.nil? || (desc_str.respond_to?(:empty?) && desc_str.empty?)
      notify_all "✔️ #{desc_str || self.class} (#{status.total})   |  #{status.bid}"

      if single_mode
        notify_all '🔶 Single mode - skipping next_node'
        return
      end

      next_node_class = next_node
      if next_node_class && (next_node_class.respond_to?(:present?) ? next_node_class.present? : !next_node_class.nil?)
        notify_all "➕ #{next_node_class} "
        next_node_class.perform_async
      else
        notify_all "✅ Конец графа #{sidekiq_queue}."
      end
    end

    # Дефолтный execute для нод без execute блока
    def execute(*args, **kwargs)
      # Пустая реализация - ноды могут работать без execute
    end

    private

    # Сохраняет BID текущего батча в Redis для ноды
    def save_bid_to_database!(bid)
      return unless bid
      
      begin
        if respond_to?(:pipeline_name) && respond_to?(:node_name)
          PipelineStatus.save_bid!(pipeline_name, node_name, bid)
          Sidekiq.logger.debug "💾 Saved BID #{bid} to Redis for #{pipeline_name}::#{node_name}"
        end
      rescue StandardError => e
        Sidekiq.logger.warn "⚠️ Failed to save BID #{bid} to Redis: #{e.message}"
      end
    end

    def notify_all(msg)
      prefix = "[#{sidekiq_queue}] "
      Sidekiq.logger.info (prefix + msg).colorize(:blue)
      custom_notifiers(prefix, msg)
    end

    def sidekiq_queue
      self.class.get_sidekiq_options['queue']
    end
  end
end

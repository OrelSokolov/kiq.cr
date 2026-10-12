module Sidekiq
  module NodeDsl
    def node(name, base = Sidekiq::Node, &block)
      # Регистрируем метаданные узла перед созданием класса
      if block_given? && block.source_location
        block_source = extract_node_block_from_file(block.source_location, name)
        description = extract_description_from_source(block_source)
        next_node_val = extract_next_node_from_source(block_source)
        jobs = extract_jobs_from_source(block_source)
        
        module_name = self.name
        metadata = {
          description: description,
          next_node: next_node_val,
          jobs: jobs,
          has_observer: block_source.include?('observer'),
          has_execute: block_source.include?('execute')
        }
        
        Sidekiq::PipelineDslExtension.register_node(module_name, name, metadata)
      end
      
      klass = Class.new(base)
      klass.class_eval(&block) if block_given?
      Sidekiq.logger.debug "Setting node #{self}::#{name}"
      const_set(name, klass)
    end
    
    private
    
    def extract_node_block_from_file(source_location, node_name)
      return "" unless source_location

      file_path, line_number = source_location
      return "" unless file_path && File.exist?(file_path)

      lines = File.readlines(file_path)
      
      # Найдем строку с определением нашего узла
      node_start_line = nil
      lines.each_with_index do |line, index|
        if line.strip.match(/node\s+[:"]#{node_name}/)
          node_start_line = index
          break
        end
      end

      return "" unless node_start_line

      # Извлекаем блок от node до соответствующего end
      block_lines = []
      bracket_count = 0
      in_node_block = false

      lines[node_start_line..-1].each do |line|
        if line.strip.match(/node\s+[:"]#{node_name}/)
          in_node_block = true
          block_lines << line
          bracket_count += 1 if line.include?(' do')
          next
        end

        if in_node_block
          block_lines << line
          
          # Подсчитываем блоки
          bracket_count += 1 if line.strip.end_with?(' do')
          bracket_count -= 1 if line.strip == 'end'

          # Если достигли конца нашего блока узла
          if bracket_count == 0
            break
          end
        end
      end

      block_lines.join
    end

    def extract_description_from_source(source)
      match = source.match(/desc\s+["']([^"']+)["']/)
      match ? match[1] : ""
    end

    def extract_next_node_from_source(source)
      match = source.match(/next_node\s+:?(\w+)/)
      return nil if match && match[1] == 'nil'
      match ? match[1] : nil
    end

    def extract_jobs_from_source(source)
      jobs = []
      
      # Простой regex для поиска всех джобов в блоке execute
      # Ищем .perform_async после слов, заканчивающихся на Job
      job_matches = source.scan(/(\w*Job)\.perform_async/i)
      
      job_matches.each do |match|
        job_name = match.is_a?(Array) ? match[0] : match
        # Убираем модули, оставляем только название джоба
        clean_job_name = job_name.split('::').last || job_name
        jobs << clean_job_name unless jobs.include?(clean_job_name)
      end
      
      jobs.uniq
    end

    # Новый метод для создания пайплайна напрямую из батчей
    # Использование:
    #   pipeline :my_pipeline do
    #     node :start_node do
    #       desc "Start processing"
    #       execute do
    #         SomeJob.perform_async
    #       end
    #       next_node :process_node
    #     end
    #
    #     node :process_node do
    #       desc "Process data"
    #       execute do
    #         ProcessJob.perform_async
    #       end
    #       next_node :end_node
    #     end
    #
    #     node :end_node do
    #       desc "Finish"
    #       execute do
    #         FinishJob.perform_async
    #       end
    #     end
    #   end
    def pipeline(name, &block)
      # Преобразуем имя в CamelCase для имени модуля
      module_name = if defined?(ActiveSupport::Inflector)
        name.to_s.camelize
      else
        name.to_s.split('_').map(&:capitalize).join
      end
      
      # Создаем модуль для пайплайна
      pipeline_module = Module.new
      
      # Расширяем модуль NodeDsl для создания нод внутри пайплайна
      pipeline_module.extend(NodeDsl)
      
      # Включаем PipelineTracking в базовый класс Node для этого пайплайна
      base_node_class = Class.new(Sidekiq::Node) do
        include Sidekiq::PipelineTracking
      end
      
      # Устанавливаем базовый класс по умолчанию для нод этого пайплайна
      pipeline_module.define_singleton_method(:node) do |node_name, base = base_node_class, &node_block|
        # Преобразуем имя ноды в CamelCase
        node_class_name = if defined?(ActiveSupport::Inflector)
          node_name.to_s.camelize
        else
          node_name.to_s.split('_').map(&:capitalize).join
        end
        
        klass = Class.new(base)
        klass.class_eval(&node_block) if node_block
        Sidekiq.logger.debug "Setting pipeline node #{module_name}::#{node_class_name}"
        pipeline_module.const_set(node_class_name, klass)
      end
      
      # Выполняем блок для создания нод
      pipeline_module.instance_eval(&block) if block_given?
      
      # Регистрируем пайплайн в текущем модуле
      const_set(module_name, pipeline_module)
      
      Sidekiq.logger.info "Pipeline #{module_name} created with nodes: #{pipeline_module.constants.select { |c| pipeline_module.const_get(c).is_a?(Class) }.map(&:to_s).join(', ')}"
      
      pipeline_module
    end
  end
end
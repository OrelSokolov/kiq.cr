# frozen_string_literal: true

module Sidekiq
  # Хуки для DSL узлов пайплайна - перехватываем каждый вызов node()
  # Регистрирует метаданные узлов для анализа и визуализации
  module PipelineDslExtension
    # Хранилище для метаданных узлов
    @@node_registry = {}

    def self.node_registry
      @@node_registry
    end

    def self.clear_registry
      @@node_registry.clear
    end

    def self.register_node(module_name, node_name, metadata)
      @@node_registry["#{module_name}::#{node_name}"] = metadata
    end

    def self.get_pipeline_data(module_name)
      @@node_registry.select { |key, _| key.start_with?("#{module_name}::") }
                     .transform_keys { |key| key.split('::').last }
    end

    # Модуль для расширения Sidekiq::NodeDsl - добавляем хуки
    module NodeDslHook
      def node(name, base_class = nil, &block)
        # Парсим блок как строку для извлечения метаданных
        block_source = ""
        if block_given?
          # Получаем исходный код блока - только конкретный блок узла
          block_source = extract_node_block_from_file(block.source_location, name)
        end
        
        # Извлекаем метаданные из исходного кода блока
        description = extract_description_from_source(block_source)
        next_node_val = extract_next_node_from_source(block_source)
        jobs = extract_jobs_from_source(block_source)
        
        # Регистрируем узел с метаданными
        module_name = self.name
        metadata = {
          description: description,
          next_node: next_node_val,
          jobs: jobs,
          has_observer: block_source.include?('observer'),
          has_execute: block_source.include?('execute')
        }
        
        Sidekiq::PipelineDslExtension.register_node(module_name, name, metadata)
        
        # Вызываем оригинальный метод node
        super(name, base_class, &block)
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
    end

    # Класс для извлечения метаданных из блока узла
    class MetadataExtractor
      attr_reader :description, :next_node

      def initialize
        @description = ""
        @next_node = nil
        @has_observer = false
        @has_execute = false
      end

      def desc(text)
        @description = text
      end

      def next_node(node_name = nil)
        @next_node = node_name
      end

      def observer(&block)
        @has_observer = true
      end

      def execute(&block)
        @has_execute = true
      end

      def has_observer?
        @has_observer
      end

      def has_execute?
        @has_execute
      end

      # Игнорируем все остальные вызовы методов в блоке
      def method_missing(method_name, *args, &block)
        # Просто игнорируем для извлечения метаданных
      end

      def respond_to_missing?(method_name, include_private = false)
        true
      end
    end
  end
end


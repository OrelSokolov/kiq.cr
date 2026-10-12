# frozen_string_literal: true

module Sidekiq
  # Модуль для визуализации пайплайнов в виде SVG диаграмм
  module PipelineVisualizer
    class Node
      attr_accessor :name, :description, :next_node, :module_name, :jobs, :x, :y, :width, :height

      def initialize(name, description, next_node, module_name, jobs = [])
        @name = name
        @description = description
        @next_node = next_node
        @module_name = module_name
        @jobs = jobs || []
        @x = 0
        @y = 0
        @width = 200  # базовая ширина
        @height = 80  # базовая высота
      end

      def id
        "#{module_name}_#{name}"
      end

      def calculate_dimensions
        # Вычисляем ширину на основе длины текста (уменьшенные коэффициенты для компактности)
        name_width = (@name.length * 8) + 60 # 8px на символ + отступы
        desc_width = @description ? (@description.length * 6) + 60 : 100 # 6px на символ для описания

        # Учитываем ширину для джобов
        jobs_width = 0
        if @jobs && @jobs.any?
          max_job_name = @jobs.max_by(&:length)
          jobs_width = (max_job_name.length * 6) + 60 if max_job_name
        end

        @width = [name_width, desc_width, jobs_width, 200].max # минимум 200px
        @height = 70 # уменьшенная базовая высота

        # Увеличиваем высоту для описания
        if @description && !@description.empty?
          max_chars_per_line = ((@width - 40) / 6).to_i # учитываем отступы для меньших шрифтов
          desc_lines = (@description.length.to_f / max_chars_per_line).ceil
          desc_lines = [desc_lines, 1].max # минимум 1 строка
          @height += desc_lines * 14 # 14px на строку описания
        end

        # Увеличиваем высоту для джобов
        return unless @jobs && @jobs.any?

        @height += (@jobs.length * 12) + 8 # 12px на джоб + отступ
      end
    end

    class Pipeline
      attr_reader :name, :nodes, :colors

      def initialize(name)
        @name = name
        @nodes = {}
        @colors = {
          'Transfermarkt' => '#96A78D',
          'Transfermarkt::Old' => '#A78D8D',
          'Rustat' => '#96A78D',
          'Rustat::Priority' => '#A78D96',
          'Rustat::PriorityFrequent' => '#8DA796',
          'Rustat::PriorityRare' => '#A78DA7',
          'Datahub' => '#96A78D',
          'Skillcorner' => '#8D96A7',
          'MOSFF' => '#A7968D'
        }
      end

      def add_node(name, description, next_node, module_name, jobs = [])
        @nodes[name] = Node.new(name, description, next_node, module_name, jobs)
      end

      def get_root_node
        # Ищем узел с именем "RootNode" - это стандартное имя корневого узла
        root_candidates = %w[RootNode CustomTeamRootNode].map { |name| nodes[name] }.compact

        return root_candidates.first if root_candidates.any?

        # Если не найден стандартный корневой узел, находим узел, на который никто не ссылается
        referenced = nodes.values.map(&:next_node).compact
        nodes.values.find { |node| !referenced.include?(node.name) }
      end

      def calculate_layout
        root = get_root_node
        return unless root

        # Сначала вычисляем размеры всех узлов
        nodes.values.each(&:calculate_dimensions)

        x_offset = 40
        y_offset = 80
        spacing_x = 30 # горизонтальный отступ между узлами

        # Горизонтальное размещение - следуем цепочке узлов слева направо
        current_node = root
        current_x = x_offset
        visited = Set.new

        while current_node && !visited.include?(current_node.name)
          visited.add(current_node.name)

          current_node.x = current_x
          current_node.y = y_offset

          # Следующий узел размещаем после текущего с учетом его ширины
          current_x += current_node.width + spacing_x

          # Переходим к следующему узлу
          next_node_name = current_node.next_node.to_s if current_node.next_node
          break unless next_node_name && nodes[next_node_name]

          current_node = nodes[next_node_name]

        end

        # Размещаем оставшиеся узлы, которые не связаны с основной цепочкой
        remaining_nodes = nodes.values - visited.map { |name| nodes[name] }.compact
        max_main_height = visited.map { |name| nodes[name] }.compact.map(&:height).max || 120

        remaining_nodes.each_with_index do |node, index|
          node.x = x_offset + index * (node.width + spacing_x)
          node.y = y_offset + max_main_height + 80 # Ниже основной цепочки
        end
      end

      def to_svg(active_nodes: Set.new, pipeline_running: false, node_statuses: {})
        calculate_layout

        svg_width = nodes.values.map { |node| node.x + node.width }.max + 30
        svg_height = nodes.values.map { |node| node.y + node.height }.max + 30

        svg = <<~SVG
          <?xml version="1.0" encoding="UTF-8"?>
          <svg width="#{svg_width}" height="#{svg_height}" xmlns="http://www.w3.org/2000/svg">
            <defs>
              <style>
                .node-rect {#{' '}
                  fill: #{colors[name] || '#f8f9fa'};#{' '}
                  stroke: #2C3E50;#{' '}
                  stroke-width: 1;
                }
                .node-rect-running {#{' '}
                  fill: #80c7ff;#{' '}
                  stroke: #0a58ca;#{' '}
                  stroke-width: 3;
                  filter: drop-shadow(3px 3px 6px rgba(13,110,253,0.4));
                }
                .node-rect-completed {#{' '}
                  fill: #fff3cd;#{' '}
                  stroke: #FFA500;#{' '}
                  stroke-width: 2;
                  filter: drop-shadow(2px 2px 4px rgba(255,215,0,0.3));
                }
                .node-text {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;#{' '}
                  font-size: 18px;#{' '}
                  font-weight: bold;
                  text-anchor: middle;
                }
                .node-text-running {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;#{' '}
                  font-size: 18px;#{' '}
                  font-weight: bold;
                  text-anchor: middle;
                }
                .node-text-completed {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;#{' '}
                  font-size: 18px;#{' '}
                  font-weight: bold;
                  text-anchor: middle;
                }
                .node-desc {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;#{' '}
                  font-size: 12px;#{' '}
                  text-anchor: middle;
                }
                .node-desc-running {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;#{' '}
                  font-size: 12px;#{' '}
                  font-weight: bold;
                  text-anchor: middle;
                }
                .node-desc-completed {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;#{' '}
                  font-size: 12px;#{' '}
                  font-weight: bold;
                  text-anchor: middle;
                }
                .job-text {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Consolas', 'Monaco', monospace;#{' '}
                  font-size: 10px;#{' '}
                  text-anchor: start;
                }
                .job-text-running {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Consolas', 'Monaco', monospace;#{' '}
                  font-size: 10px;#{' '}
                  font-weight: bold;
                  text-anchor: start;
                }
                .job-text-completed {#{' '}
                  fill: #000000;#{' '}
                  font-family: 'Consolas', 'Monaco', monospace;#{' '}
                  font-size: 10px;#{' '}
                  font-weight: bold;
                  text-anchor: start;
                }
                .separator-line {
                  stroke: #000;
                  stroke-width: 2;
                  opacity: 1;
                }
                .separator-line-running {
                  stroke: #000;
                  stroke-width: 2;
                  opacity: 1;
                }
                .separator-line-completed {
                  stroke: #000;
                  stroke-width: 2;
                  opacity: 1;
                }
                .edge {#{' '}
                  stroke: #34495E;#{' '}
                  stroke-width: 3;#{' '}
                  marker-end: url(#arrowhead);
                }
                .edge-active {#{' '}
                  stroke: #0d6efd;#{' '}
                  stroke-width: 4;#{' '}
                  marker-end: url(#arrowhead-active);
                }
              </style>
              <marker id="arrowhead" markerWidth="6" markerHeight="4"#{' '}
               refX="5" refY="2" orient="auto">
                <polygon points="0 0, 6 2, 0 4" fill="#34495E" />
              </marker>
              <marker id="arrowhead-active" markerWidth="6" markerHeight="4"#{' '}
               refX="5" refY="2" orient="auto">
                <polygon points="0 0, 6 2, 0 4" fill="#0d6efd" />
              </marker>
            </defs>
        SVG

        # Рисуем связи между узлами
        nodes.values.each do |node|
          next unless node.next_node

          next_node_name = node.next_node.to_s
          next_node = nodes[next_node_name]
          next unless next_node

          from_x = node.x + node.width # правый край узла
          from_y = node.y + (node.height / 2) # центр по вертикали
          to_x = next_node.x # левый край следующего узла
          to_y = next_node.y + (next_node.height / 2) # центр по вертикали

          # Определяем, активна ли связь (если пайплайн запущен И один из узлов активен)
          is_edge_active = pipeline_running && (active_nodes.include?(node.id) || active_nodes.include?(next_node.id))
          edge_class = is_edge_active ? 'edge-active' : 'edge'

          svg += <<~SVG
            <line x1="#{from_x}" y1="#{from_y}" x2="#{to_x}" y2="#{to_y}" class="#{edge_class}" />
          SVG
        end

        # Рисуем узлы
        nodes.values.each do |node|
          # Определяем статус узла из node_statuses, если он есть
          node_status = node_statuses[node.id] || (active_nodes.include?(node.id) ? 'completed' : 'pending')

          # Определяем классы на основе статуса
          case node_status
          when 'running'
            rect_class = 'node-rect-running'
            text_class = 'node-text-running'
            desc_class = 'node-desc-running'
            job_text_class = 'job-text-running'
            separator_class = 'separator-line-running'
          when 'completed'
            rect_class = 'node-rect-completed'
            text_class = 'node-text-completed'
            desc_class = 'node-desc-completed'
            job_text_class = 'job-text-completed'
            separator_class = 'separator-line-completed'
          else
            rect_class = 'node-rect'
            text_class = 'node-text'
            desc_class = 'node-desc'
            job_text_class = 'job-text'
            separator_class = 'separator-line'
          end

          svg += <<~SVG
            <rect x="#{node.x}" y="#{node.y}" width="#{node.width}" height="#{node.height}" rx="25" ry="25" class="#{rect_class}" />
          SVG

          # Название узла
          text_x = node.x + (node.width / 2)
          text_y = node.y + 22
          svg += <<~SVG
            <text x="#{text_x}" y="#{text_y}" class="#{text_class}">#{node.name}</text>
          SVG

          # Описание узла (многострочный текст)
          current_y = text_y + 18
          if node.description && !node.description.empty?
            # Вычисляем максимальную длину строки на основе ширины узла
            max_chars_per_line = ((node.width - 40) / 6).to_i

            words = node.description.split(' ')
            lines = []
            current_line = ''

            words.each do |word|
              test_line = current_line.empty? ? word : "#{current_line} #{word}"
              if test_line.length > max_chars_per_line
                lines << current_line unless current_line.empty?
                current_line = word
              else
                current_line = test_line
              end
            end
            lines << current_line unless current_line.empty?

            lines.each_with_index do |line, index|
              desc_y = current_y + (index * 14)
              svg += <<~SVG
                <text x="#{text_x}" y="#{desc_y}" class="#{desc_class}">#{line}</text>
              SVG
            end
            current_y += lines.length * 14
          end

          # Список джобов
          next unless node.jobs && node.jobs.any?

          # Горизонтальная разделительная линия
          current_y += 4 # уменьшенный отступ перед линией
          line_x1 = node.x + 15 # отступ от левого края
          line_x2 = node.x + node.width - 15 # отступ от правого края
          svg += <<~SVG
            <line x1="#{line_x1}" y1="#{current_y}" x2="#{line_x2}" y2="#{current_y}" class="#{separator_class}" />
          SVG

          current_y += 14 # уменьшенный отступ после линии
          job_x = node.x + 15 # уменьшенный отступ от левого края узла
          node.jobs.each_with_index do |job, index|
            job_y = current_y + (index * 12)
            svg += <<~SVG
              <text x="#{job_x}" y="#{job_y}" class="#{job_text_class}">• #{job}</text>
            SVG
          end
        end

        svg += '</svg>'
        svg
      end

      def get_title(pipeline_running: false)
        "#{name} Pipeline#{pipeline_running ? '' : ' - не запущен'}"
      end
    end

    class Collector
      def self.collect_pipelines
        pipelines = {}

        # Собираем все зарегистрированные узлы
        all_nodes = Sidekiq::PipelineDslExtension.node_registry

        # Группируем узлы по модулям
        nodes_by_module = all_nodes.group_by { |key, _| key.split('::').first }

        # Для каждого модуля ищем корневые узлы (заканчиваются на RootNode)
        nodes_by_module.each do |module_name, module_nodes|
          # Находим все корневые узлы в этом модуле
          root_nodes = module_nodes.select { |key, _| key.end_with?('RootNode') }

          root_nodes.each do |root_node_key, _root_metadata|
            # Определяем pipeline_id на основе имени корневого узла
            node_name = root_node_key.split('::').last
            pipeline_id = determine_pipeline_id(module_name, node_name)
            pipeline_display_name = determine_pipeline_display_name(module_name, node_name)

            # Создаем пайплайн
            pipeline = Pipeline.new(pipeline_display_name)

            # Собираем все узлы для этого пайплайна, следуя по цепочке next_node
            collect_pipeline_nodes(pipeline, root_node_key, all_nodes, pipeline_id)

            # Добавляем в результат если есть узлы
            pipelines[pipeline_id] = pipeline if pipeline.nodes.any?
          end
        end

        pipelines
      end

      def self.determine_pipeline_id(module_name, root_node_name)
        # Определяем уникальный ID для пайплайна
        case root_node_name
        when 'RootNode'
          underscore(module_name)
        when 'PriorityFrequentRootNode'
          "#{underscore(module_name)}_priority_frequent"
        when 'PriorityRareRootNode'
          "#{underscore(module_name)}_priority_rare"
        else
          # Для других корневых узлов создаем ID на основе имени
          suffix = root_node_name.gsub('RootNode', '')
          if defined?(ActiveSupport::Inflector)
            "#{underscore(module_name)}_#{suffix.underscore}"
          else
            "#{underscore(module_name)}_#{suffix.gsub(/([A-Z])/, '_\1').downcase.gsub(/^_/, '')}"
          end
        end
      end

      def self.underscore(string)
        string.to_s.gsub(/::/, '/')
              .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
              .gsub(/([a-z\d])([A-Z])/, '\1_\2')
              .tr('-', '_')
              .downcase
      end

      def self.determine_pipeline_display_name(module_name, root_node_name)
        # Определяем отображаемое имя для пайплайна
        case root_node_name
        when 'RootNode'
          module_name
        when 'PriorityFrequentRootNode'
          "#{module_name}::PriorityFrequent"
        when 'PriorityRareRootNode'
          "#{module_name}::PriorityRare"
        else
          # Для других корневых узлов создаем имя на основе имени узла
          suffix = root_node_name.gsub('RootNode', '')
          "#{module_name}::#{suffix}"
        end
      end

      def self.collect_pipeline_nodes(pipeline, current_node_key, all_nodes, pipeline_id)
        visited = Set.new
        current_key = current_node_key

        while current_key && !visited.include?(current_key)
          visited.add(current_key)

          # Получаем метаданные узла
          metadata = all_nodes[current_key]
          break unless metadata

          # Извлекаем имя узла и модуль
          node_name = current_key.split('::').last
          module_name = current_key.split('::')[0..-2].join('::')
          module_name = current_key.split('::').first if module_name.empty?

          # Добавляем узел в пайплайн
          # Используем pipeline_id в качестве module_name для правильной идентификации
          pipeline.add_node(
            node_name,
            metadata[:description] || '',
            metadata[:next_node],
            pipeline_id, # Используем pipeline_id вместо module_name
            metadata[:jobs] || []
          )

          # Переходим к следующему узлу
          next_node_name = metadata[:next_node]
          break unless next_node_name

          # Ищем следующий узел в том же модуле
          current_key = all_nodes.keys.find do |key|
            key.start_with?("#{current_key.split('::').first}::") &&
              key.end_with?("::#{next_node_name}")
          end
        end
      end
    end
  end
end

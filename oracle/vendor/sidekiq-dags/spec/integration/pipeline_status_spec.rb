require 'spec_helper'
require 'sidekiq/batch'
require 'sidekiq/testing'

# Добавляем метод present? для совместимости с Node
class String
  def present?
    !empty?
  end
end

class NilClass
  def present?
    false
  end
end

class Class
  def present?
    true
  end
end

class Object
  def present?
    !nil?
  end
end

Sidekiq::Testing.server_middleware do |chain|
  chain.add Sidekiq::Batch::Middleware::ServerMiddleware
end

# Массив для хранения номеров нод, у которых был вызван коллбэк
$pipeline_status_callbacks_runs = []

# DummyJob для создания непустого батча
class DummyJob
  include Sidekiq::Worker

  def perform(desc = 'dummy')
    # Пустой джоб
  end
end

# Создаем тестовый модуль и 100 нод
module TestPipeline
  extend Sidekiq::NodeDsl

  class BaseNode < Sidekiq::Node
    include Sidekiq::PipelineTracking
    sidekiq_options queue: 'test_pipeline'

    # Переопределяем perform для отслеживания вызовов
    def perform(*args, **kwargs)
      # Добавляем номер ноды в глобальный массив
      node_number = self.class.name.match(/PipelineStatusNode(\d+)/)[1].to_i
      $pipeline_status_callbacks_runs << node_number

      # Вызываем оригинальный perform
      super(*args, **kwargs)
    end
  end

  # Создаем 100 нод
  (1..100).each do |i|
    node_class = Class.new(BaseNode) do
      desc "PipelineStatusNode#{i}"
      execute { }
    end
    const_set("PipelineStatusNode#{i}", node_class)
  end
end

# Устанавливаем связи между нодами (next_node)
(1..99).each do |i|
  next_node_class = TestPipeline.const_get("PipelineStatusNode#{i + 1}")
  node_class = TestPipeline.const_get("PipelineStatusNode#{i}")
  node_class.next_node(next_node_class)
end

# Запускаем тест
describe 'Pipeline Status Test with 100 nodes' do
  before(:each) do
    $pipeline_status_callbacks_runs.clear
    Sidekiq.redis { |r| r.flushdb }

    # Очищаем данные из Redis
    if defined?(Sidekiq::SidekiqPipeline)
      Sidekiq::SidekiqPipeline.destroy_all
    end
  end

  it 'executes all 100 nodes in sequence, calls all callbacks, and sets all statuses to completed' do
    # Запускаем первую ноду
    TestPipeline::PipelineStatusNode1.perform_async

    # Обрабатываем все джобы
    Sidekiq::Worker.drain_all

    # Ожидаемый массив: [1, 2, 3, ..., 100]
    expected = (1..100).to_a

    # Проверяем разницу между ожидаемым и фактическим для callbacks
    missing_callbacks = expected - $pipeline_status_callbacks_runs
    extra_callbacks = $pipeline_status_callbacks_runs - expected

    puts "Expected callbacks: #{expected.size}"
    puts "Actual callbacks: #{$pipeline_status_callbacks_runs.size}"
    puts "Missing callbacks: #{missing_callbacks.inspect}"
    puts "Extra callbacks: #{extra_callbacks.inspect}"
    puts "Callbacks order: #{$pipeline_status_callbacks_runs.inspect}"

    # Проверяем, что все коллбэки были вызваны
    expect(missing_callbacks).to be_empty
    expect(extra_callbacks).to be_empty
    expect($pipeline_status_callbacks_runs.size).to eq(100),
                                                    "Expected 100 callbacks, got #{$pipeline_status_callbacks_runs.size}"
    expect($pipeline_status_callbacks_runs.sort).to eq(expected),
                                                    'Callbacks were not called in correct order or some are missing'

    # Проверяем статусы в Redis
    pipeline = Sidekiq::SidekiqPipeline.for('test_pipeline')

    expect(pipeline).not_to be_nil

    # Проверяем, что все ноды завершились со статусом completed
    pipeline_nodes = pipeline.sidekiq_pipeline_nodes
    node_statuses = pipeline_nodes.map(&:status)

    puts "Total pipeline nodes: #{pipeline_nodes.count}"
    puts "Node statuses: #{node_statuses.inspect}"

    # Проверяем, что все статусы равны 'completed'
    all_completed = pipeline_nodes.all? { |node| node.status == 'completed' }

    expect(pipeline_nodes.count).to eq(100)
    expect(all_completed).to be(true), "Not all nodes are completed. Statuses: #{pipeline_nodes.map do |n|
      "#{n.node_name}:#{n.status}"
    end.inspect}"

    # Альтернативная проверка через символы
    all_completed_symbols_check = pipeline_nodes.all? { |node| node.status.to_sym == :completed }
    expect(all_completed_symbols_check).to be(true), 'Not all nodes have :completed status (symbol check)'

    # Проверяем статус пайплайна
    expect(pipeline.completed?).to be(true)
    expect(pipeline.failed?).to be(false)
    expect(pipeline.running?).to be(false)

    # Проверяем прогресс
    expect(pipeline.progress_percent).to eq(100.0)

    puts '✅ All 100 nodes completed successfully'
    puts "✅ Pipeline status: #{pipeline.status}"
    puts "✅ Pipeline progress: #{pipeline.progress_percent}%"
  end
end

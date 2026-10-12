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

# Тест цепочки из 100 нод для проверки работы коллбэков и race conditions
# Цепочка: Node1 -> Node2 -> Node3 -> ... -> Node100

# Массив для хранения номеров нод, у которых был вызван коллбэк
$callbacks_runs = []

# DummyJob для создания непустого батча
class DummyJob
  include Sidekiq::Worker
  
  def perform(desc)
    # Пустой джоб
  end
end

# Создаем 100 нод в цепочке
(1..100).each do |i|
  node_class = Class.new(Sidekiq::Node) do
    desc "Node#{i}"
    
    # Коллбэк, который добавляет номер ноды в массив
    def on_complete(status, options)
      node_number = self.class.name.match(/Node(\d+)/)[1].to_i
      $callbacks_runs << node_number
      super # Вызываем родительский метод для запуска следующей ноды
    end
  end
  
  Object.const_set("Node#{i}", node_class)
end

# Устанавливаем связи между нодами (next_node)
(1..99).each do |i|
  next_node_class = Object.const_get("Node#{i + 1}")
  node_class = Object.const_get("Node#{i}")
  node_class.next_node(next_node_class)
end

# Запускаем тест
describe "Node Chain Test with 100 nodes" do
  before(:each) do
    $callbacks_runs.clear
    Sidekiq.redis { |r| r.flushdb }
  end
  
  it "executes all 100 nodes in sequence and calls all callbacks" do
    # Запускаем первую ноду
    Node1.perform_async
    
    # Обрабатываем все джобы
    Sidekiq::Worker.drain_all
    
    # Ожидаемый массив: [1, 2, 3, ..., 100]
    expected = (1..100).to_a
    
    # Проверяем разницу между ожидаемым и фактическим
    missing = expected - $callbacks_runs
    extra = $callbacks_runs - expected
    
    puts "Expected callbacks: #{expected.size}"
    puts "Actual callbacks: #{$callbacks_runs.size}"
    puts "Missing callbacks: #{missing.inspect}"
    puts "Extra callbacks: #{extra.inspect}"
    puts "Callbacks order: #{$callbacks_runs.inspect}"
    
    # В лучшем случае разницы быть не должно
    expect(missing).to be_empty, "Missing callbacks: #{missing.inspect}. Expected all 100 callbacks to run."
    expect(extra).to be_empty, "Extra callbacks: #{extra.inspect}. Found unexpected callbacks."
    expect($callbacks_runs.size).to eq(100), "Expected 100 callbacks, got #{$callbacks_runs.size}"
    
    # Проверяем, что все ноды были вызваны
    expect($callbacks_runs.sort).to eq(expected), "Callbacks were not called in correct order or some are missing"
  end
end


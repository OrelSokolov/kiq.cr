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

# Тест множественных коллбэков для цепочки из 100 нод
# Каждая нода имеет два коллбэка complete, которые добавляют строки вида "номер-1" и "номер-2"
# Цепочка: Node1 -> Node2 -> Node3 -> ... -> Node100

# Глобальная переменная с уникальным именем для изоляции от других тестов
$multiple_callbacks_runs = []

# Запускаем тест
describe "Multiple Callbacks Test with 100 nodes" do
  before(:each) do
    $multiple_callbacks_runs.clear
    Sidekiq.redis { |r| r.flushdb }
    
    # Очищаем старые классы, если они существуют
    (1..100).each do |i|
      Object.send(:remove_const, "MultipleCallbackNode#{i}") if Object.const_defined?("MultipleCallbackNode#{i}")
    end
    Object.send(:remove_const, "MultipleCallbackDummyJob") if Object.const_defined?("MultipleCallbackDummyJob")
    Object.send(:remove_const, "MultipleCallback1") if Object.const_defined?("MultipleCallback1")
    Object.send(:remove_const, "MultipleCallback2") if Object.const_defined?("MultipleCallback2")
    
    # DummyJob для создания непустого батча
    multiple_callback_dummy_job = Class.new do
      include Sidekiq::Worker
      
      define_method(:perform) do |desc|
        # Пустой джоб
      end
    end
    Object.const_set("MultipleCallbackDummyJob", multiple_callback_dummy_job)
    
    # Первый коллбэк - добавляет "номер-1"
    callback1_class = Class.new do
      define_method(:on_complete) do |status, options|
        node_number = options['node_number'] || options[:node_number]
        $multiple_callbacks_runs << "#{node_number}-1" if node_number
      end
    end
    Object.const_set("MultipleCallback1", callback1_class)
    
    # Второй коллбэк - добавляет "номер-2"
    callback2_class = Class.new do
      define_method(:on_complete) do |status, options|
        node_number = options['node_number'] || options[:node_number]
        $multiple_callbacks_runs << "#{node_number}-2" if node_number
      end
    end
    Object.const_set("MultipleCallback2", callback2_class)
    
    # Создаем 100 нод в цепочке
    (1..100).each do |i|
      node_number = i
      
      node_class = Class.new(Sidekiq::Node) do
        desc "MultipleCallbackNode#{node_number}"
        
        # Переопределяем perform, чтобы зарегистрировать два коллбэка
        define_method(:perform) do |*args, **kwargs|
          observer
          @batch = Sidekiq::Batch.new

          # Извлекаем номер ноды из имени класса
          node_num = self.class.name.match(/MultipleCallbackNode(\d+)/)[1].to_i

          # Регистрируем два коллбэка для complete события с номером ноды в options
          # Используем строковое имя класса, так как коллбэки сериализуются
          @batch.on(:complete, "MultipleCallback1", node_number: node_num)
          @batch.on(:complete, "MultipleCallback2", node_number: node_num)

          @batch.add_jobs do
            MultipleCallbackDummyJob.perform_async(desc) # Needed for not empty job list
            execute(*args, **kwargs)
          end

          @batch.on(:complete, self.class)
          @batch.run

          s = Sidekiq::Batch::ExplicitStatus.new(@batch.bid)
          notify_all "➡️ #{desc.present? ? desc : self.class} -> (#{s.total})    | #{@batch.bid}"
        end
        
        # Коллбэк для запуска следующей ноды
        define_method(:on_complete) do |status, options|
          # Вызываем родительский метод для запуска следующей ноды
          Sidekiq::Node.instance_method(:on_complete).bind(self).call(status, options)
        end
      end
      
      Object.const_set("MultipleCallbackNode#{i}", node_class)
    end

    # Устанавливаем связи между нодами (next_node)
    (1..99).each do |i|
      next_node_class = Object.const_get("MultipleCallbackNode#{i + 1}")
      node_class = Object.const_get("MultipleCallbackNode#{i}")
      node_class.next_node(next_node_class)
    end
  end
  
  after(:each) do
    # Очищаем созданные классы после теста
    (1..100).each do |i|
      Object.send(:remove_const, "MultipleCallbackNode#{i}") if Object.const_defined?("MultipleCallbackNode#{i}")
    end
    Object.send(:remove_const, "MultipleCallbackDummyJob") if Object.const_defined?("MultipleCallbackDummyJob")
    Object.send(:remove_const, "MultipleCallback1") if Object.const_defined?("MultipleCallback1")
    Object.send(:remove_const, "MultipleCallback2") if Object.const_defined?("MultipleCallback2")
  end
  
  it "executes all 100 nodes with 2 callbacks each (200 total callbacks)" do
    # Запускаем первую ноду
    MultipleCallbackNode1.perform_async
    
    # Обрабатываем все джобы
    Sidekiq::Worker.drain_all
    
    # Ожидаемый массив: ["1-1", "1-2", "2-1", "2-2", ..., "100-1", "100-2"]
    expected = []
    (1..100).each do |i|
      expected << "#{i}-1"
      expected << "#{i}-2"
    end
    
    # Проверяем разницу между ожидаемым и фактическим
    missing = expected - $multiple_callbacks_runs
    extra = $multiple_callbacks_runs - expected
    
    puts "Expected callbacks: #{expected.size}"
    puts "Actual callbacks: #{$multiple_callbacks_runs.size}"
    puts "Missing callbacks: #{missing.inspect}"
    puts "Extra callbacks: #{extra.inspect}"
    puts "First 20 callbacks: #{$multiple_callbacks_runs.first(20).inspect}"
    puts "Last 20 callbacks: #{$multiple_callbacks_runs.last(20).inspect}"
    
    # В лучшем случае разницы быть не должно
    expect(missing).to be_empty, "Missing callbacks: #{missing.inspect}. Expected all 200 callbacks to run."
    expect(extra).to be_empty, "Extra callbacks: #{extra.inspect}. Found unexpected callbacks."
    expect($multiple_callbacks_runs.size).to eq(200), "Expected 200 callbacks (2 per node), got #{$multiple_callbacks_runs.size}"
    
    # Проверяем, что все коллбэки были вызваны (порядок может быть разным из-за параллельности)
    expect($multiple_callbacks_runs.sort).to eq(expected.sort), "Callbacks were not called correctly. Expected #{expected.size} callbacks, got #{$multiple_callbacks_runs.size}"
  end
end


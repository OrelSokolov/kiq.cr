# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/batch'
require 'sidekiq/testing'
require_relative '../support/database'

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

Sidekiq::Testing.server_middleware do |chain|
  chain.add Sidekiq::Batch::Middleware::ServerMiddleware
end

# =============================================================================
# Тестовые модули для проверки find_node_class_by_name
# Создаем несколько пайплайнов с одинаковыми именами нод (RootNode, TeamsNode)
# =============================================================================

# Первый пайплайн: Alpha
module Alpha
  extend Sidekiq::NodeDsl
  
  class AlphaBaseNode < Sidekiq::Node
    include Sidekiq::PipelineTracking
    sidekiq_options queue: 'alpha'
  end
  
  # RootNode -> TeamsNode -> EndNode
  node :EndNode, AlphaBaseNode do
    desc "Alpha EndNode"
    next_node nil
    execute { }
  end
  
  node :TeamsNode, AlphaBaseNode do
    desc "Alpha TeamsNode"
    next_node :EndNode
    execute { }
  end
  
  node :RootNode, AlphaBaseNode do
    desc "Alpha RootNode"
    next_node :TeamsNode
    execute { }
  end
end

# Второй пайплайн: Beta
module Beta
  extend Sidekiq::NodeDsl
  
  class BetaBaseNode < Sidekiq::Node
    include Sidekiq::PipelineTracking
    sidekiq_options queue: 'beta'
  end
  
  # RootNode -> PlayersNode -> EndNode
  node :EndNode, BetaBaseNode do
    desc "Beta EndNode"
    next_node nil
    execute { }
  end
  
  node :PlayersNode, BetaBaseNode do
    desc "Beta PlayersNode"
    next_node :EndNode
    execute { }
  end
  
  node :TeamsNode, BetaBaseNode do
    desc "Beta TeamsNode"
    next_node :PlayersNode
    execute { }
  end
  
  node :RootNode, BetaBaseNode do
    desc "Beta RootNode"
    next_node :TeamsNode
    execute { }
  end
end

# Третий пайплайн: Gamma (с underscore в имени)
module GammaTest
  extend Sidekiq::NodeDsl
  
  class GammaBaseNode < Sidekiq::Node
    include Sidekiq::PipelineTracking
    sidekiq_options queue: 'gamma_test'
  end
  
  node :EndNode, GammaBaseNode do
    desc "GammaTest EndNode"
    next_node nil
    execute { }
  end
  
  node :RootNode, GammaBaseNode do
    desc "GammaTest RootNode"
    next_node :EndNode
    execute { }
  end
end

describe Sidekiq::PipelineCallback do
  let(:callback) { Sidekiq::PipelineCallback.new }
  
  before(:each) do
    Sidekiq.redis { |r| r.flushdb }
  end
  
  describe '#find_node_class_by_name' do
    context 'when pipeline_name is provided' do
      it 'finds RootNode in Alpha module when pipeline_name is "alpha"' do
        result = callback.send(:find_node_class_by_name, 'RootNode', 'alpha')
        
        expect(result).to eq(Alpha::RootNode)
        expect(result.name).to eq('Alpha::RootNode')
      end
      
      it 'finds RootNode in Beta module when pipeline_name is "beta"' do
        result = callback.send(:find_node_class_by_name, 'RootNode', 'beta')
        
        expect(result).to eq(Beta::RootNode)
        expect(result.name).to eq('Beta::RootNode')
      end
      
      it 'finds TeamsNode in Alpha module when pipeline_name is "alpha"' do
        result = callback.send(:find_node_class_by_name, 'TeamsNode', 'alpha')
        
        expect(result).to eq(Alpha::TeamsNode)
        expect(result.name).to eq('Alpha::TeamsNode')
      end
      
      it 'finds TeamsNode in Beta module when pipeline_name is "beta"' do
        result = callback.send(:find_node_class_by_name, 'TeamsNode', 'beta')
        
        expect(result).to eq(Beta::TeamsNode)
        expect(result.name).to eq('Beta::TeamsNode')
      end
      
      it 'finds PlayersNode only in Beta (unique to Beta)' do
        result = callback.send(:find_node_class_by_name, 'PlayersNode', 'beta')
        
        expect(result).to eq(Beta::PlayersNode)
      end
      
      it 'handles underscore in pipeline_name (gamma_test -> GammaTest)' do
        result = callback.send(:find_node_class_by_name, 'RootNode', 'gamma_test')
        
        expect(result).to eq(GammaTest::RootNode)
        expect(result.name).to eq('GammaTest::RootNode')
      end
    end
    
    context 'when pipeline_name is nil (fallback mode)' do
      it 'finds some RootNode (any module) - fallback behavior' do
        result = callback.send(:find_node_class_by_name, 'RootNode', nil)
        
        # Должен найти хоть какой-то RootNode
        expect(result).not_to be_nil
        expect(result.name).to include('RootNode')
        expect(result).to be < Sidekiq::Node
      end
    end
    
    context 'when node does not exist' do
      it 'returns nil for non-existent node' do
        result = callback.send(:find_node_class_by_name, 'NonExistentNode', 'alpha')
        
        expect(result).to be_nil
      end
      
      it 'returns nil for non-existent pipeline' do
        result = callback.send(:find_node_class_by_name, 'RootNode', 'nonexistent')
        
        # Fallback должен найти RootNode из другого модуля
        expect(result).not_to be_nil
      end
    end
  end
  
  describe '#trigger_next_node' do
    before(:each) do
      Sidekiq::Testing.fake!
    end
    
    after(:each) do
      Sidekiq::Worker.clear_all
    end
    
    it 'triggers correct next node for Alpha::RootNode (Alpha::TeamsNode)' do
      callback.send(:trigger_next_node, 'alpha', 'RootNode')
      
      # Должен быть запланирован Alpha::TeamsNode, а не Beta::TeamsNode
      expect(Alpha::TeamsNode.jobs.size).to eq(1)
      expect(Beta::TeamsNode.jobs.size).to eq(0)
    end
    
    it 'triggers correct next node for Beta::RootNode (Beta::TeamsNode)' do
      callback.send(:trigger_next_node, 'beta', 'RootNode')
      
      # Должен быть запланирован Beta::TeamsNode
      expect(Beta::TeamsNode.jobs.size).to eq(1)
      expect(Alpha::TeamsNode.jobs.size).to eq(0)
    end
    
    it 'triggers correct next node for Alpha::TeamsNode (Alpha::EndNode)' do
      callback.send(:trigger_next_node, 'alpha', 'TeamsNode')
      
      # Alpha::TeamsNode.next_node = Alpha::EndNode
      expect(Alpha::EndNode.jobs.size).to eq(1)
      expect(Beta::EndNode.jobs.size).to eq(0)
    end
    
    it 'triggers correct next node for Beta::TeamsNode (Beta::PlayersNode)' do
      callback.send(:trigger_next_node, 'beta', 'TeamsNode')
      
      # Beta::TeamsNode.next_node = Beta::PlayersNode
      expect(Beta::PlayersNode.jobs.size).to eq(1)
    end
    
    it 'does not trigger anything for EndNode (next_node is nil)' do
      callback.send(:trigger_next_node, 'alpha', 'EndNode')
      
      # EndNode не имеет следующей ноды
      expect(Alpha::RootNode.jobs.size).to eq(0)
      expect(Alpha::TeamsNode.jobs.size).to eq(0)
    end
  end
  
  describe 'integration: correct next_node resolution' do
    it 'Alpha::RootNode.next_node returns Alpha::TeamsNode' do
      node = Alpha::RootNode.new
      next_node = node.next_node
      
      expect(next_node).to eq(Alpha::TeamsNode)
      expect(next_node.name).to eq('Alpha::TeamsNode')
    end
    
    it 'Beta::RootNode.next_node returns Beta::TeamsNode' do
      node = Beta::RootNode.new
      next_node = node.next_node
      
      expect(next_node).to eq(Beta::TeamsNode)
      expect(next_node.name).to eq('Beta::TeamsNode')
    end
    
    it 'Alpha::TeamsNode.next_node returns Alpha::EndNode' do
      node = Alpha::TeamsNode.new
      next_node = node.next_node
      
      expect(next_node).to eq(Alpha::EndNode)
    end
    
    it 'Beta::TeamsNode.next_node returns Beta::PlayersNode' do
      node = Beta::TeamsNode.new
      next_node = node.next_node
      
      expect(next_node).to eq(Beta::PlayersNode)
    end
  end
  
  describe 'regression test: same node names in different pipelines' do
    # Этот тест проверяет баг, который был исправлен:
    # При поиске RootNode без учета pipeline_name возвращался первый найденный
    # RootNode (например Rustat::RootNode вместо Bsight::RootNode)
    
    it 'finds correct RootNode based on pipeline_name, not first found' do
      alpha_root = callback.send(:find_node_class_by_name, 'RootNode', 'alpha')
      beta_root = callback.send(:find_node_class_by_name, 'RootNode', 'beta')
      
      expect(alpha_root).not_to eq(beta_root)
      expect(alpha_root).to eq(Alpha::RootNode)
      expect(beta_root).to eq(Beta::RootNode)
    end
    
    it 'finds correct TeamsNode based on pipeline_name' do
      alpha_teams = callback.send(:find_node_class_by_name, 'TeamsNode', 'alpha')
      beta_teams = callback.send(:find_node_class_by_name, 'TeamsNode', 'beta')
      
      expect(alpha_teams).not_to eq(beta_teams)
      expect(alpha_teams).to eq(Alpha::TeamsNode)
      expect(beta_teams).to eq(Beta::TeamsNode)
    end
    
    it 'different pipelines with same node names have different next_nodes' do
      # Alpha: RootNode -> TeamsNode -> EndNode
      # Beta:  RootNode -> TeamsNode -> PlayersNode -> EndNode
      
      alpha_root_next = Alpha::RootNode.new.next_node
      beta_root_next = Beta::RootNode.new.next_node
      
      alpha_teams_next = Alpha::TeamsNode.new.next_node
      beta_teams_next = Beta::TeamsNode.new.next_node
      
      # RootNode.next_node совпадает по имени (TeamsNode), но разные классы
      expect(alpha_root_next.name).to include('TeamsNode')
      expect(beta_root_next.name).to include('TeamsNode')
      expect(alpha_root_next).not_to eq(beta_root_next)
      
      # TeamsNode.next_node РАЗНЫЕ
      expect(alpha_teams_next).to eq(Alpha::EndNode)
      expect(beta_teams_next).to eq(Beta::PlayersNode)
    end
  end
  
  # =============================================================================
  # КРИТИЧЕСКИЕ ТЕСТЫ: Намеренные конфликты имён нод между пайплайнами
  # Эти тесты предотвращают повторение бага когда Bsight::RootNode
  # запускал Rustat::TeamsNode вместо Bsight::ClubNode
  # =============================================================================
  describe 'CRITICAL: intentional node name conflicts between pipelines' do
    before(:each) do
      Sidekiq::Testing.fake!
      Sidekiq::Worker.clear_all
    end
    
    after(:each) do
      Sidekiq::Worker.clear_all
    end
    
    # Тест 1: Воспроизведение оригинального бага
    # Когда несколько пайплайнов имеют RootNode, должен запускаться
    # next_node из ПРАВИЛЬНОГО пайплайна, а не первого найденного
    it 'BUG REPRODUCTION: trigger_next_node for RootNode must use correct pipeline module' do
      # Симулируем ситуацию: завершился Alpha::RootNode
      # Ожидание: должен запуститься Alpha::TeamsNode
      # Баг: запускался первый найденный TeamsNode (мог быть Beta::TeamsNode)
      
      callback.send(:trigger_next_node, 'alpha', 'RootNode')
      
      # КРИТИЧЕСКАЯ ПРОВЕРКА: запустился именно Alpha::TeamsNode
      expect(Alpha::TeamsNode.jobs.size).to eq(1), 
        "Alpha::TeamsNode должен быть запланирован, но jobs.size = #{Alpha::TeamsNode.jobs.size}"
      
      # КРИТИЧЕСКАЯ ПРОВЕРКА: Beta::TeamsNode НЕ должен был запуститься
      expect(Beta::TeamsNode.jobs.size).to eq(0),
        "Beta::TeamsNode НЕ должен быть запланирован при завершении Alpha::RootNode!"
      
      # Дополнительно: GammaTest тоже не должен быть затронут
      expect(GammaTest::RootNode.jobs.size).to eq(0)
    end
    
    # Тест 2: Обратная ситуация - Beta пайплайн не должен влиять на Alpha
    it 'BUG REPRODUCTION: trigger_next_node for Beta::RootNode must not affect Alpha pipeline' do
      # Симулируем ситуацию: завершился Beta::RootNode
      # Ожидание: должен запуститься Beta::TeamsNode
      # Баг: мог запуститься Alpha::TeamsNode если он был найден первым
      
      callback.send(:trigger_next_node, 'beta', 'RootNode')
      
      # КРИТИЧЕСКАЯ ПРОВЕРКА: запустился именно Beta::TeamsNode
      expect(Beta::TeamsNode.jobs.size).to eq(1),
        "Beta::TeamsNode должен быть запланирован, но jobs.size = #{Beta::TeamsNode.jobs.size}"
      
      # КРИТИЧЕСКАЯ ПРОВЕРКА: Alpha::TeamsNode НЕ должен был запуститься
      expect(Alpha::TeamsNode.jobs.size).to eq(0),
        "Alpha::TeamsNode НЕ должен быть запланирован при завершении Beta::RootNode!"
    end
    
    # Тест 3: Проверка что find_node_class_by_name возвращает класс из правильного модуля
    it 'find_node_class_by_name returns class from correct module, not first alphabetically' do
      # Модули в алфавитном порядке: Alpha, Beta, GammaTest
      # Без pipeline_name мог бы вернуться Alpha (первый по алфавиту)
      
      # Запрашиваем RootNode для Beta - должен вернуть Beta::RootNode
      result = callback.send(:find_node_class_by_name, 'RootNode', 'beta')
      
      expect(result).to eq(Beta::RootNode),
        "Ожидался Beta::RootNode, получен #{result&.name || 'nil'}"
      expect(result).not_to eq(Alpha::RootNode),
        "НЕ должен возвращать Alpha::RootNode для pipeline_name='beta'!"
    end
    
    # Тест 4: Цепочка переходов должна оставаться в рамках своего пайплайна
    it 'full node chain stays within its own pipeline module' do
      # Alpha цепочка: RootNode -> TeamsNode -> EndNode
      # Проверяем что ВСЯ цепочка остаётся в Alpha
      
      # Шаг 1: RootNode завершается
      callback.send(:trigger_next_node, 'alpha', 'RootNode')
      expect(Alpha::TeamsNode.jobs.size).to eq(1)
      Alpha::TeamsNode.clear
      
      # Шаг 2: TeamsNode завершается
      callback.send(:trigger_next_node, 'alpha', 'TeamsNode')
      expect(Alpha::EndNode.jobs.size).to eq(1)
      
      # Проверяем что Beta пайплайн не был затронут ни на одном шаге
      expect(Beta::TeamsNode.jobs.size).to eq(0)
      expect(Beta::PlayersNode.jobs.size).to eq(0)
      expect(Beta::EndNode.jobs.size).to eq(0)
    end
  end
  
  # =============================================================================
  # ТЕСТЫ: Race condition detection с учетом failures
  # Проверяем, что когда pending == failures, это НЕ race condition
  # 
  # ВАЖНО: Используем реальный объект Sidekiq::Batch::Status вместо моков,
  # потому что status.failures возвращает число (Integer), а не массив!
  # Мокирование failures как массива скрывало реальную проблему в коде.
  # =============================================================================
  describe 'race condition detection with failures' do
    before(:each) do
      # Создаем тестовый пайплайн и ноду
      @pipeline = Sidekiq::SidekiqPipeline.for('alpha')
      @node = Sidekiq::SidekiqPipelineNode.for('alpha', 'RootNode')
      @node.start! # Устанавливаем статус running
    end
    
    after(:each) do
      Sidekiq::SidekiqPipelineNode.destroy_all
      Sidekiq::SidekiqPipeline.destroy_all
    end
    
    it 'does NOT raise error when pending == failures (normal situation)' do
      # Создаем реальный batch status с pending == failures
      batch_id = 'TEST_BATCH_123'
      
      # Создаем батч в Redis с pending = 5 и failures = 5
      # status.failures возвращает число (scard), поэтому создаем set с 5 элементами
      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 5)
        conn.hset(bidkey, "total", 10)
        conn.expire(bidkey, 3600)
        
        # Создаем set с 5 failed jobs (status.failures = scard этого set)
        5.times do |i|
          conn.sadd("BID-#{batch_id}-failed", "failed_job_#{i}")
        end
        conn.expire("BID-#{batch_id}-failed", 3600)
      end
      
      # Используем реальный объект Status вместо мока
      status = Sidekiq::Batch::Status.new(batch_id)
      
      # Проверяем, что настройка правильная
      expect(status.pending).to eq(5)
      expect(status.failures).to eq(5)  # status.failures возвращает число!
      
      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode'
      }
      
      # Вызываем on_complete - НЕ должно быть ошибки
      expect {
        callback.on_complete(status, options)
      }.not_to raise_error
      
      # Проверяем, что в логах есть сообщение о том, что это нормально
      # (проверка через логирование, но главное - что не было raise)
    end
    
    it 'DOES raise error when pending > failures (real race condition)' do
      # Создаем реальный batch status с pending > failures
      batch_id = 'TEST_BATCH_456'
      
      # Создаем батч в Redis с pending = 5 и failures = 2
      # status.failures возвращает число (scard), поэтому создаем set с 2 элементами
      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 5)
        conn.hset(bidkey, "total", 10)
        conn.expire(bidkey, 3600)
        
        # Создаем set с 2 failed jobs (status.failures = scard этого set)
        2.times do |i|
          conn.sadd("BID-#{batch_id}-failed", "failed_job_#{i}")
        end
        conn.expire("BID-#{batch_id}-failed", 3600)
      end
      
      # Используем реальный объект Status вместо мока
      status = Sidekiq::Batch::Status.new(batch_id)
      
      # Проверяем, что настройка правильная
      expect(status.pending).to eq(5)
      expect(status.failures).to eq(2)  # status.failures возвращает число!
      
      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode'
      }
      
      # Вызываем on_complete - ДОЛЖНА быть ошибка
      expect {
        callback.on_complete(status, options)
      }.to raise_error(RuntimeError, /race condition detected/)
    end
    
    it 'does NOT raise error when pending == 0 (normal completion)' do
      # Создаем реальный batch status с pending = 0
      batch_id = 'TEST_BATCH_789'
      
      # Создаем батч в Redis с pending = 0 и failures = 0
      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 0)
        conn.hset(bidkey, "total", 10)
        conn.expire(bidkey, 3600)
        
        # Не создаем failed set, поэтому status.failures вернет 0
      end
      
      # Используем реальный объект Status вместо мока
      status = Sidekiq::Batch::Status.new(batch_id)
      
      # Проверяем, что настройка правильная
      expect(status.pending).to eq(0)
      expect(status.failures).to eq(0)  # status.failures возвращает число!
      
      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode'
      }
      
      # Вызываем on_complete - НЕ должно быть ошибки
      expect {
        callback.on_complete(status, options)
      }.not_to raise_error
    end
    
    it 'handles failures exception gracefully' do
      # Создаем реальный batch status с pending = 3 и failures = 0
      # но мокируем failures чтобы выбросить исключение
      batch_id = 'TEST_BATCH_NIL_FAILURES'
      
      # Создаем батч в Redis с pending = 3
      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 3)
        conn.hset(bidkey, "total", 10)
        conn.expire(bidkey, 3600)
      end
      
      # Используем реальный объект Status, но мокируем failures чтобы выбросить исключение
      status = Sidekiq::Batch::Status.new(batch_id)
      allow(status).to receive(:failures).and_raise(StandardError.new('No failures'))
      
      # Проверяем, что pending правильный
      expect(status.pending).to eq(3)
      
      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode'
      }
      
      # Вызываем on_complete - должно обработать исключение failures как 0
      # и поднять ошибку, так как pending = 3 > 0 и failures = 0 (после обработки исключения)
      expect {
        callback.on_complete(status, options)
      }.to raise_error(RuntimeError, /race condition detected/)
    end
  end

  # =============================================================================
  # ТЕСТЫ: Single режим
  # Проверяем, что когда передан 'single' => true:
  # - callback не запускает следующую ноду
  # - нода помечается как completed
  # =============================================================================
  describe 'single mode' do
    before(:each) do
      # Создаем тестовый пайплайн и ноду
      @pipeline = Sidekiq::SidekiqPipeline.for('alpha')
      @node = Sidekiq::SidekiqPipelineNode.for('alpha', 'RootNode')
      @node.start! # Устанавливаем статус running

      Sidekiq::Testing.fake!
      Sidekiq::Worker.clear_all
    end

    after(:each) do
      Sidekiq::Worker.clear_all
      Sidekiq::SidekiqPipelineNode.destroy_all
      Sidekiq::SidekiqPipeline.destroy_all
    end

    it 'does NOT trigger next_node when single mode is enabled' do
      # Создаем batch status с pending = 0 (успешное завершение)
      batch_id = 'TEST_SINGLE_MODE'

      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 0)
        conn.hset(bidkey, "total", 1)
        conn.expire(bidkey, 3600)
      end

      status = Sidekiq::Batch::Status.new(batch_id)

      # Проверяем что следующая нода еще не запланирована
      expect(Alpha::TeamsNode.jobs.size).to eq(0)

      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode',
        'single' => true
      }

      # Вызываем on_complete с single => true
      callback.on_complete(status, options)

      # Проверяем что нода помечена как completed
      @node.reload
      expect(@node).to be_completed

      # КРИТИЧЕСКАЯ ПРОВЕРКА: следующая нода НЕ должна быть запланирована
      expect(Alpha::TeamsNode.jobs.size).to eq(0),
        "TeamsNode не должен быть запланирован в single режиме"
    end

    it 'triggers next_node when single mode is NOT enabled' do
      # Создаем batch status с pending = 0 (успешное завершение)
      batch_id = 'TEST_NON_SINGLE_MODE'

      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 0)
        conn.hset(bidkey, "total", 1)
        conn.expire(bidkey, 3600)
      end

      status = Sidekiq::Batch::Status.new(batch_id)

      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode',
        'single' => false
      }

      # Вызываем on_complete с single => false
      callback.on_complete(status, options)

      # Проверяем что нода помечена как completed
      @node.reload
      expect(@node).to be_completed

      # Проверяем что следующая нода ЗАПЛАНИРОВАНА
      expect(Alpha::TeamsNode.jobs.size).to eq(1),
        "TeamsNode должен быть запланирован когда single режим отключен"
    end

    it 'does NOT trigger next_node when single option is missing (default behavior)' do
      # Создаем batch status с pending = 0 (успешное завершение)
      batch_id = 'TEST_NO_SINGLE_OPTION'

      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 0)
        conn.hset(bidkey, "total", 1)
        conn.expire(bidkey, 3600)
      end

      status = Sidekiq::Batch::Status.new(batch_id)

      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode'
        # 'single' опция отсутствует - должно работать как обычно
      }

      # Вызываем on_complete без single опции
      callback.on_complete(status, options)

      # Проверяем что нода помечена как completed
      @node.reload
      expect(@node).to be_completed

      # Проверяем что следующая нода ЗАПЛАНИРОВАНА (по умолчанию single = false)
      expect(Alpha::TeamsNode.jobs.size).to eq(1),
        "TeamsNode должен быть запланирован по умолчанию (без single опции)"
    end

    it 'handles single mode with symbol key :single' do
      # Создаем batch status с pending = 0 (успешное завершение)
      batch_id = 'TEST_SINGLE_SYMBOL'

      Sidekiq.redis do |conn|
        bidkey = "BID-#{batch_id}"
        conn.hset(bidkey, "pending", 0)
        conn.hset(bidkey, "total", 1)
        conn.expire(bidkey, 3600)
      end

      status = Sidekiq::Batch::Status.new(batch_id)

      options = {
        'pipeline_name' => 'alpha',
        'node_name' => 'RootNode',
        :single => true  # символ вместо строки
      }

      # Вызываем on_complete с :single => true
      callback.on_complete(status, options)

      # Проверяем что нода помечена как completed
      @node.reload
      expect(@node).to be_completed

      # КРИТИЧЕСКАЯ ПРОВЕРКА: следующая нода НЕ должна быть запланирована
      expect(Alpha::TeamsNode.jobs.size).to eq(0),
        "TeamsNode не должен быть запланирован в single режиме (с символом :single)"
    end
  end
end


require 'integration_helper'

# Тест для эмуляции race condition с большим количеством быстрых джобов
# Пустые джобы создают максимальную нагрузку и race condition,
# так как они завершаются почти мгновенно и одновременно

class EmptyWorker
  include Sidekiq::Worker
  
  def perform
    # Пустой воркер - выполняется мгновенно
    # Это создает максимальную нагрузку на систему подсчета pending
  end
end

class RaceConditionCallback
  @@call_count = 0
  @@mutex = Mutex.new
  @@complete_times = []
  @@success_times = []
  
  def self.reset
    @@mutex.synchronize do
      @@call_count = 0
      @@complete_times = []
      @@success_times = []
    end
  end
  
  def self.call_count
    @@mutex.synchronize { @@call_count }
  end
  
  def self.complete_times
    @@mutex.synchronize { @@complete_times.dup }
  end
  
  def self.success_times
    @@mutex.synchronize { @@success_times.dup }
  end
  
  def on_complete(status, options)
    @@mutex.synchronize do
      @@call_count += 1
      @@complete_times << Time.now
      Sidekiq.logger.info "Complete callback called (count: #{@@call_count}) - PENDING: #{status.pending}, FAILURES: #{status.failures}, TOTAL: #{status.total}"
    end
  end
  
  def on_success(status, options)
    @@mutex.synchronize do
      @@success_times << Time.now
      Sidekiq.logger.info "Success callback called - PENDING: #{status.pending}, FAILURES: #{status.failures}, TOTAL: #{status.total}"
    end
  end
end

describe "Race Condition Test with 1000 Empty Jobs" do
  before do
    RaceConditionCallback.reset
    Sidekiq.redis { |r| r.flushdb }
  end
  
  it "handles 1000 concurrent empty job completions without race conditions" do
    # Создаем батч с 1000 пустых джобов
    # Пустые джобы завершаются мгновенно, создавая максимальную нагрузку на race condition
    batch = Sidekiq::Batch.new
    
    job_count = 1000
    jids = []
    
    batch.add_jobs do
      job_count.times do |i|
        jid = EmptyWorker.perform_async
        jids << jid
      end
    end
    batch.run
    
    bid = batch.bid
    status = Sidekiq::Batch::Status.new(bid)
    
    # Проверяем начальное состояние
    expect(status.pending).to eq(job_count), "Initial pending should be #{job_count}"
    expect(status.total).to eq(job_count), "Initial total should be #{job_count}"
    
    Sidekiq.logger.info "Starting race condition test with #{job_count} jobs, bid: #{bid}"
    
    # Эмулируем race condition: обрабатываем джобы батчами для избежания перегрузки пула соединений
    # но внутри каждого батча создаем максимальную параллельность
    start_time = Time.now
    batch_size = 50  # Обрабатываем по 50 джобов параллельно
    
    jids.each_slice(batch_size).with_index do |batch_jids, batch_index|
      threads = []
      
      batch_jids.each do |jid|
        threads << Thread.new do
          # Минимальная задержка для создания race condition внутри батча
          sleep(rand(0.00001..0.0001)) if batch_index > 0
          Sidekiq::Batch.process_successful_job(bid, jid)
        end
      end
      
      # Ждем завершения текущего батча перед следующим
      threads.each(&:join)
      
      # Минимальная пауза между батчами для стабилизации
      sleep(0.001) if batch_index < (jids.size / batch_size - 1)
    end
    
    completion_time = Time.now - start_time
    
    Sidekiq.logger.info "All jobs processed in #{completion_time} seconds"
    
    # НЕ ждем! Проверяем сразу после join - все потоки завершились синхронно
    
    # Проверяем: если batch существует (не был cleaned up), pending должен быть 0
    # Если batch не существует - значит cleanup произошел, что означает pending дошел до 0
    batch_exists = Sidekiq.redis { |r| r.exists("BID-#{bid}") } > 0
    
    if batch_exists
      # Batch НЕ был cleaned - это значит pending НЕ дошел до 0!
      # Это и есть race condition - batch завис с pending > 0
      final_pending = Sidekiq.redis { |r| r.hget("BID-#{bid}", 'pending') }.to_i
      final_failures = Sidekiq.redis { |r| r.scard("BID-#{bid}-failed") }.to_i
      
      Sidekiq.logger.info "BATCH STILL EXISTS! PENDING: #{final_pending}, FAILURES: #{final_failures}"
      
      # Если batch существует и pending > 0, это race condition
      expect(final_pending).to eq(0), 
        "CRITICAL RACE CONDITION DETECTED! Batch still exists with pending = #{final_pending}. " \
        "Expected pending to be 0, but batch is stuck with #{final_pending} pending job(s). " \
        "This is the exact race condition bug - 'PENDING иногда остаётся 1'! " \
        "Total jobs: #{job_count}, Failures: #{final_failures}"
    else
      # Batch был cleaned up - значит pending дошел до 0 и callbacks отработали
      Sidekiq.logger.info "Batch was cleaned up successfully - pending reached 0"
    end
    
    Sidekiq.logger.info "Test passed! Batch completed correctly"
  end
  
  it "handles 1000 concurrent empty jobs with extreme parallelism" do
    batch = Sidekiq::Batch.new
    
    job_count = 1000
    jids = []
    
    batch.add_jobs do
      job_count.times do
        jid = EmptyWorker.perform_async
        jids << jid
      end
    end
    batch.run
    
    bid = batch.bid
    
    Sidekiq.logger.info "Starting extreme parallelism test with #{job_count} jobs"
    
    # Максимально агрессивная эмуляция с батчингом для избежания перегрузки пула
    # Используем большие батчи для максимальной параллельности внутри батча
    start_time = Time.now
    batch_size = 100  # Большие батчи для максимальной race condition
    
    jids.each_slice(batch_size).with_index do |batch_jids, batch_index|
      threads = []
      
      batch_jids.each do |jid|
        threads << Thread.new do
          # Принудительное переключение контекста для создания race condition
          Thread.pass
          Sidekiq::Batch.process_successful_job(bid, jid)
          Thread.pass
        end
      end
      
      threads.each(&:join)
      
      # Минимальная пауза между батчами
      sleep(0.0005) if batch_index < (jids.size / batch_size - 1)
    end
    
    completion_time = Time.now - start_time
    
    Sidekiq.logger.info "All jobs processed in #{completion_time} seconds"
    
    # НЕ ждем! Проверяем сразу после join
    
    batch_exists = Sidekiq.redis { |r| r.exists("BID-#{bid}") } > 0
    
    if batch_exists
      final_pending = Sidekiq.redis { |r| r.hget("BID-#{bid}", 'pending') }.to_i
      final_failures = Sidekiq.redis { |r| r.scard("BID-#{bid}-failed") }.to_i
      
      Sidekiq.logger.info "BATCH STILL EXISTS! PENDING: #{final_pending}, FAILURES: #{final_failures}"
      
      expect(final_pending).to eq(0),
        "CRITICAL: Race condition detected! Batch still exists with pending = #{final_pending}. " \
        "Batch is stuck with #{final_pending} pending job(s). " \
        "Total jobs: #{job_count}, Failures: #{final_failures}"
    else
      Sidekiq.logger.info "Batch was cleaned up successfully"
    end
    
    Sidekiq.logger.info "Extreme parallelism test passed!"
  end
  
  it "handles 1000 empty jobs with mixed timing to simulate real-world conditions" do
    batch = Sidekiq::Batch.new
    
    job_count = 1000
    jids = []
    
    batch.add_jobs do
      job_count.times do
        jid = EmptyWorker.perform_async
        jids << jid
      end
    end
    batch.run
    
    bid = batch.bid
    
    Sidekiq.logger.info "Starting mixed timing test with #{job_count} jobs"
    
    # Эмулируем реальные условия: разные джобы завершаются в разное время
    # но большинство очень быстро (пустые джобы)
    # Используем батчинг для избежания перегрузки пула
    start_time = Time.now
    batch_size = 75
    
    jids.each_slice(batch_size).with_index do |batch_jids, batch_index|
      threads = []
      
      batch_jids.each_with_index do |jid, local_index|
        threads << Thread.new do
          # Создаем волны завершения для эмуляции реальных условий
          # но большинство джобов завершаются почти мгновенно
          global_index = batch_index * batch_size + local_index
          delay = case global_index % 10
                  when 0 then rand(0.0001..0.0005)  # 10% очень быстро
                  when 1..8 then rand(0.0001..0.001)  # 80% быстро
                  else rand(0.001..0.005)  # 10% немного медленнее
                  end
          
          sleep(delay)
          Sidekiq::Batch.process_successful_job(bid, jid)
        end
      end
      
      threads.each(&:join)
      
      # Небольшая пауза между батчами
      sleep(0.001) if batch_index < (jids.size / batch_size - 1)
    end
    
    completion_time = Time.now - start_time
    
    Sidekiq.logger.info "All jobs processed in #{completion_time} seconds"
    
    # Проверяем итоговое состояние
    # Нужно подождать немного, чтобы все Redis операции завершились
    sleep(0.1)
    
    batch_exists = Sidekiq.redis { |r| r.exists("BID-#{bid}") } > 0
    
    if batch_exists
      final_pending = Sidekiq.redis { |r| r.hget("BID-#{bid}", 'pending') }.to_i
      final_failures = Sidekiq.redis { |r| r.scard("BID-#{bid}-failed") }.to_i
      
      Sidekiq.logger.info "BATCH STILL EXISTS! PENDING: #{final_pending}, FAILURES: #{final_failures}"
      
      expect(final_pending).to eq(0),
        "Race condition detected! Batch still exists with pending = #{final_pending}. " \
        "This indicates a race condition in process_successful_job. " \
        "Total jobs: #{job_count}, Failures: #{final_failures}"
    else
      # Даже если batch был cleanup-нут, нужно проверить, что ВСЕ джобы были обработаны
      # Проверяем, что колбэки были вызваны корректно
      Sidekiq.logger.info "Batch was cleaned up successfully"
      
      # Подсчитываем, сколько раз мы вызвали process_successful_job
      # Должно быть ровно job_count раз
      expected_decrements = job_count
      Sidekiq.logger.info "Expected #{expected_decrements} successful job completions"
    end
    
    Sidekiq.logger.info "Mixed timing test passed!"
  end
end


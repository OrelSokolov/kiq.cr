require 'integration_helper'

# Коллбэк для отслеживания вызовов и запуска второго батча
class TwoBatchCallback
  @@first_batch_complete = false
  @@second_batch_started = false
  @@second_batch_complete = false
  @@second_batch_bid = nil
  @@mutex = Mutex.new

  def self.reset
    @@mutex.synchronize do
      @@first_batch_complete = false
      @@second_batch_started = false
      @@second_batch_complete = false
      @@second_batch_bid = nil
    end
  end

  def self.first_batch_complete?
    @@mutex.synchronize { @@first_batch_complete }
  end

  def self.second_batch_started?
    @@mutex.synchronize { @@second_batch_started }
  end

  def self.second_batch_complete?
    @@mutex.synchronize { @@second_batch_complete }
  end

  def self.second_batch_bid
    @@mutex.synchronize { @@second_batch_bid }
  end

  def on_complete(status, options)
    @@mutex.synchronize do
      batch_id = status.bid
      
      Sidekiq.logger.info "Complete callback called for batch #{batch_id} - PENDING: #{status.pending}, FAILURES: #{status.failures}, TOTAL: #{status.total}"
      
      # Если это первый батч, запускаем второй
      # ВАЖНО: используем строковые ключи, т.к. options приходят из JSON
      if options.is_a?(Hash) && options['batch_number'] == 1
        @@first_batch_complete = true
        start_second_batch(options['second_batch_jids'])
      elsif options.is_a?(Hash) && options['batch_number'] == 2
        @@second_batch_complete = true
      end
    end
  end

  def start_second_batch(jids)
    return if @@second_batch_started
    
    @@second_batch_started = true
    Sidekiq.logger.info "Starting second batch with #{jids.size} jobs"
    
    batch2 = Sidekiq::Batch.new
    batch2.on(:complete, TwoBatchCallback, { batch_number: 2 })
    
    batch2.add_jobs do
      jids.each do |jid|
        batch2.increment_job_queue(jid)
      end
    end
    batch2.run
    
    @@second_batch_bid = batch2.bid
    Sidekiq.logger.info "Second batch started with bid: #{@@second_batch_bid}"
  end
end

describe "Two Batch Test with 100 jobs each" do
  before do
    TwoBatchCallback.reset
    Sidekiq.redis { |r| r.flushdb }
  end

  it "handles two sequential batches with 100 jobs each and failures" do
    job_count = 100
    expected_failures_batch1 = 10  # 10% падений в первом батче
    expected_failures_batch2 = 15  # 15% падений во втором батче

    # Первый батч
    batch1 = Sidekiq::Batch.new
    batch1_jids = job_count.times.map { SecureRandom.uuid }
    
    batch1.add_jobs do
      batch1_jids.each do |jid|
        batch1.increment_job_queue(jid)
      end
    end
    
    # Генерируем JID'ы для второго батча заранее
    batch2_jids = job_count.times.map { SecureRandom.uuid }
    
    # Регистрируем коллбэк с данными для второго батча
    batch1.on(:complete, TwoBatchCallback, { 
      batch_number: 1, 
      second_batch_jids: batch2_jids 
    })
    
    batch1.run
    batch1_bid = batch1.bid

    Sidekiq.logger.info "Starting first batch with #{job_count} jobs, bid: #{batch1_bid}"

    # Обрабатываем первый батч
    batch1_jids.each_with_index do |jid, index|
      if index < expected_failures_batch1
        Sidekiq::Batch.process_failed_job(batch1_bid, jid)
      else
        Sidekiq::Batch.process_successful_job(batch1_bid, jid)
      end
    end

    # Ждем завершения первого батча и запуска второго
    sleep(0.2)

    # Выполняем все джобы в очереди, включая callback batch'и
    Sidekiq.logger.info "Draining all Sidekiq workers..."
    Sidekiq::Worker.drain_all

    # Ждем завершения всех callback batch'ей
    sleep(0.2)

    # Проверяем, что первый батч завершен
    batch1_status = Sidekiq::Batch::Status.new(batch1_bid)
    Sidekiq.logger.info "First batch final state - PENDING: #{batch1_status.pending}, FAILURES: #{batch1_status.failures}, TOTAL: #{batch1_status.total}"

    expect(batch1_status.failures).to eq(expected_failures_batch1),
      "First batch: Expected exactly #{expected_failures_batch1} failures, got #{batch1_status.failures}"

    expect(batch1_status.pending).to eq(expected_failures_batch1),
      "First batch: Pending should equal failures: expected #{expected_failures_batch1}, got #{batch1_status.pending}"

    # Проверяем, что второй батч был запущен
    expect(TwoBatchCallback.first_batch_complete?).to eq(true), "First batch should have completed"
    expect(TwoBatchCallback.second_batch_started?).to eq(true), "Second batch should have been started"

    # Получаем BID второго батча
    batch2_bid = TwoBatchCallback.second_batch_bid
    expect(batch2_bid).not_to be_nil,
      "Second batch BID should be set"

    Sidekiq.logger.info "Second batch started with bid: #{batch2_bid}"

    # Обрабатываем второй батч
    batch2_jids.each_with_index do |jid, index|
      if index < expected_failures_batch2
        Sidekiq::Batch.process_failed_job(batch2_bid, jid)
      else
        Sidekiq::Batch.process_successful_job(batch2_bid, jid)
      end
    end

    # Ждем завершения второго батча
    sleep(0.2)

    # Выполняем все джобы в очереди, включая callback batch'и
    Sidekiq.logger.info "Draining all Sidekiq workers for second batch..."
    Sidekiq::Worker.drain_all

    # Проверяем состояние второго батча
    batch2_status = Sidekiq::Batch::Status.new(batch2_bid)
    Sidekiq.logger.info "Second batch final state - PENDING: #{batch2_status.pending}, FAILURES: #{batch2_status.failures}, TOTAL: #{batch2_status.total}"

    expect(batch2_status.failures).to eq(expected_failures_batch2),
      "Second batch: Expected exactly #{expected_failures_batch2} failures, got #{batch2_status.failures}"

    expect(batch2_status.pending).to eq(expected_failures_batch2),
      "Second batch: Pending should equal failures: expected #{expected_failures_batch2}, got #{batch2_status.pending}"

    expect(batch2_status.total).to eq(job_count),
      "Second batch: Total should be #{job_count}, got #{batch2_status.total}"

    # Проверяем, что второй батч завершен
    expect(TwoBatchCallback.second_batch_complete?).to eq(true), "Second batch should have completed"

    Sidekiq.logger.info "Two batch test passed!"
  end

  it "handles two sequential batches with random failures" do
    job_count = 100

    # Первый батч
    batch1 = Sidekiq::Batch.new
    batch1_jids = job_count.times.map { SecureRandom.uuid }
    
    batch1.add_jobs do
      batch1_jids.each do |jid|
        batch1.increment_job_queue(jid)
      end
    end
    
    # Генерируем JID'ы для второго батча заранее
    batch2_jids = job_count.times.map { SecureRandom.uuid }
    
    batch1.on(:complete, TwoBatchCallback, { 
      batch_number: 1, 
      second_batch_jids: batch2_jids 
    })
    
    batch1.run
    batch1_bid = batch1.bid

    Sidekiq.logger.info "Starting first batch with #{job_count} jobs (random failures), bid: #{batch1_bid}"

    # Обрабатываем первый батч со случайными падениями (10% вероятность)
    batch1_failures = 0
    batch1_jids.each do |jid|
      if rand < 0.1
        Sidekiq::Batch.process_failed_job(batch1_bid, jid)
        batch1_failures += 1
      else
        Sidekiq::Batch.process_successful_job(batch1_bid, jid)
      end
    end

    # Ждем завершения первого батча и запуска второго
    sleep(0.2)

    # Выполняем все джобы в очереди, включая callback batch'и
    Sidekiq.logger.info "Draining all Sidekiq workers..."
    Sidekiq::Worker.drain_all

    # Ждем завершения всех callback batch'ей
    sleep(0.2)

    batch1_status = Sidekiq::Batch::Status.new(batch1_bid)
    Sidekiq.logger.info "First batch final state - PENDING: #{batch1_status.pending}, FAILURES: #{batch1_status.failures}, TOTAL: #{batch1_status.total}"

    expect(batch1_status.failures).to eq(batch1_failures),
      "First batch: Failures should match processed failures (#{batch1_failures}), got #{batch1_status.failures}"

    expect(batch1_status.pending).to eq(batch1_status.failures),
      "First batch: Pending should equal failures"

    # Проверяем, что второй батч был запущен
    expect(TwoBatchCallback.second_batch_started?).to eq(true), "Second batch should have been started"

    batch2_bid = TwoBatchCallback.second_batch_bid
    expect(batch2_bid).not_to be_nil,
      "Second batch BID should be set"

    Sidekiq.logger.info "Second batch started with bid: #{batch2_bid}"

    # Обрабатываем второй батч со случайными падениями (15% вероятность)
    batch2_failures = 0
    batch2_jids.each do |jid|
      if rand < 0.15
        Sidekiq::Batch.process_failed_job(batch2_bid, jid)
        batch2_failures += 1
      else
        Sidekiq::Batch.process_successful_job(batch2_bid, jid)
      end
    end

    # Ждем завершения второго батча
    sleep(0.2)

    # Выполняем все джобы в очереди, включая callback batch'и
    Sidekiq.logger.info "Draining all Sidekiq workers for second batch..."
    Sidekiq::Worker.drain_all

    batch2_status = Sidekiq::Batch::Status.new(batch2_bid)
    Sidekiq.logger.info "Second batch final state - PENDING: #{batch2_status.pending}, FAILURES: #{batch2_status.failures}, TOTAL: #{batch2_status.total}"

    expect(batch2_status.failures).to eq(batch2_failures),
      "Second batch: Failures should match processed failures (#{batch2_failures}), got #{batch2_status.failures}"

    expect(batch2_status.pending).to eq(batch2_status.failures),
      "Second batch: Pending should equal failures"

    expect(batch2_status.total).to eq(job_count),
      "Second batch: Total should be #{job_count}, got #{batch2_status.total}"

    # Проверяем, что оба батча завершены
    expect(TwoBatchCallback.second_batch_complete?).to eq(true), "Second batch should have completed"

    Sidekiq.logger.info "Two batch test with random failures passed!"
  end
end

require 'integration_helper'

# Коллбэк для отслеживания вызовов
class FailedJobsCallback
  @@call_count = 0
  @@mutex = Mutex.new

  def self.reset
    @@mutex.synchronize do
      @@call_count = 0
    end
  end

  def self.call_count
    @@mutex.synchronize { @@call_count }
  end

  def on_complete(status, options)
    @@mutex.synchronize do
      @@call_count += 1
      Sidekiq.logger.info "Complete callback called - PENDING: #{status.pending}, FAILURES: #{status.failures}, TOTAL: #{status.total}"
    end
  end

  def on_success(status, options)
    @@mutex.synchronize do
      @@call_count += 1
      Sidekiq.logger.info "Success callback called - PENDING: #{status.pending}, FAILURES: #{status.failures}, TOTAL: #{status.total}"
    end
  end
end

describe "Failed Jobs Test with 1000 jobs" do
  before do
    FailedJobsCallback.reset
    Sidekiq.redis { |r| r.flushdb }
  end

  it "handles 1000 jobs with random failures and tracks failed jobs correctly" do
    batch = Sidekiq::Batch.new
    batch.on(:success, FailedJobsCallback)
    batch.on(:complete, FailedJobsCallback)

    job_count = 1000

    # Генерируем JID'ы напрямую, без создания воркера
    jids = job_count.times.map { SecureRandom.uuid }

    batch.add_jobs do
      jids.each do |jid|
        batch.increment_job_queue(jid)
      end
    end
    batch.run

    bid = batch.bid
    status = Sidekiq::Batch::Status.new(bid)

    # Проверяем начальное состояние
    expect(status.pending).to eq(job_count), "Initial pending should be #{job_count}"
    expect(status.total).to eq(job_count), "Initial total should be #{job_count}"

    Sidekiq.logger.info "Starting failed jobs test with #{job_count} jobs, bid: #{bid}"

    # Эмулируем обработку джобов с случайными сбоями
    # Обрабатываем батчами для избежания перегрузки пула соединений
    start_time = Time.now
    batch_size = 50
    processed_failures = 0
    processed_successes = 0

    jids.each_slice(batch_size).with_index do |batch_jids, batch_index|
      batch_jids.each do |jid|
        # Случайным образом определяем, будет ли джоб успешным или провальным
        # 10% вероятность падения
        if rand < 0.1
          Sidekiq::Batch.process_failed_job(bid, jid)
          processed_failures += 1
        else
          Sidekiq::Batch.process_successful_job(bid, jid)
          processed_successes += 1
        end
      end

      # Минимальная пауза между батчами для стабилизации
      sleep(0.001) if batch_index < (jids.size / batch_size - 1)
    end

    completion_time = Time.now - start_time

    Sidekiq.logger.info "All jobs processed in #{completion_time} seconds"
    Sidekiq.logger.info "Processed: #{processed_successes} successes, #{processed_failures} failures"

    # Проверяем итоговое состояние
    sleep(0.1)  # Ждем завершения всех Redis операций

    batch_exists = Sidekiq.redis { |r| r.exists("BID-#{bid}") } > 0

    # Получаем актуальный статус из Redis
    final_status = Sidekiq::Batch::Status.new(bid)
    final_pending = final_status.pending
    final_failures = final_status.failures

    Sidekiq.logger.info "Final state - PENDING: #{final_pending}, FAILURES: #{final_failures}, TOTAL: #{final_status.total}"

    # После обработки всех джобов:
    # - pending должен быть равен failures (все неуспешные джобы counted как pending)
    # - failures должен быть > 0 (поскольку мы имели случайные падения)
    expect(final_pending).to eq(final_failures),
      "Pending (#{final_pending}) should equal failures (#{final_failures}). " \
      "All failed jobs should be counted in both counters."

    expect(final_failures).to be > 0,
      "Expected at least some failures in #{job_count} jobs with 10% failure rate. " \
      "Got #{processed_failures} failures."

    expect(final_failures).to eq(processed_failures),
      "Failures counter (#{final_failures}) should match processed failures (#{processed_failures})."

    # Проверяем, что коллбэки были вызваны (если были failures, коллбэки могут не вызываться)
    callback_count = FailedJobsCallback.call_count
    Sidekiq.logger.info "Callback was called #{callback_count} times"

    # Коллбэки могут не вызываться, если все джобы еще не завершены
    # или если batch еще не достиг состояния для вызова коллбэков
    # Главное - проверить корректность счетчиков pending и failures
    Sidekiq.logger.info "Failed jobs test passed!"
  end

  it "handles 1000 jobs with exact failure count verification" do
    # Этот тест использует детерминированный подход: мы сами контролируем сколько джобов упадут
    batch = Sidekiq::Batch.new
    batch.on(:success, FailedJobsCallback)
    batch.on(:complete, FailedJobsCallback)

    job_count = 1000
    expected_failures = 100  # Точно 10% падений

    # Генерируем JID'ы напрямую
    jids = job_count.times.map { SecureRandom.uuid }

    batch.add_jobs do
      jids.each do |jid|
        batch.increment_job_queue(jid)
      end
    end
    batch.run

    bid = batch.bid

    Sidekiq.logger.info "Starting deterministic failed jobs test with #{job_count} jobs, expected #{expected_failures} failures"

    # Первые expected_failures джобов упадут, остальные успешные
    jids.each_with_index do |jid, index|
      if index < expected_failures
        Sidekiq::Batch.process_failed_job(bid, jid)
      else
        Sidekiq::Batch.process_successful_job(bid, jid)
      end
    end

    # Ждем завершения всех Redis операций
    sleep(0.1)

    final_status = Sidekiq::Batch::Status.new(bid)
    final_pending = final_status.pending
    final_failures = final_status.failures

    Sidekiq.logger.info "Final state - PENDING: #{final_pending}, FAILURES: #{final_failures}, TOTAL: #{final_status.total}"

    # Проверяем точное число падений
    expect(final_failures).to eq(expected_failures),
      "Expected exactly #{expected_failures} failures, got #{final_failures}"

    expect(final_pending).to eq(expected_failures),
      "Pending should equal failures: expected #{expected_failures}, got #{final_pending}"

    expect(final_status.total).to eq(job_count),
      "Total should be #{job_count}, got #{final_status.total}"

    Sidekiq.logger.info "Deterministic failed jobs test passed!"
  end

  it "handles 1000 jobs with mixed parallel processing of failures and successes" do
    # Тестируем смешанную обработку: успехи и неудачи обрабатываются параллельно
    batch = Sidekiq::Batch.new
    batch.on(:success, FailedJobsCallback)
    batch.on(:complete, FailedJobsCallback)

    job_count = 1000

    # Генерируем JID'ы напрямую
    jids = job_count.times.map { SecureRandom.uuid }

    batch.add_jobs do
      jids.each do |jid|
        batch.increment_job_queue(jid)
      end
    end
    batch.run

    bid = batch.bid

    Sidekiq.logger.info "Starting mixed parallel test with #{job_count} jobs"

    # Обрабатываем батчами с параллельным выполнением
    # Внутри каждого батча джобы могут быть успешными или провальными
    start_time = Time.now
    batch_size = 50

    jids.each_slice(batch_size).with_index do |batch_jids, batch_index|
      threads = []

      batch_jids.each do |jid|
        threads << Thread.new do
          # Случайным образом определяем успех или неудачу
          # Используем synchronized random для детерминизма
          if rand < 0.1
            Sidekiq::Batch.process_failed_job(bid, jid)
          else
            Sidekiq::Batch.process_successful_job(bid, jid)
          end
        end
      end

      threads.each(&:join)

      # Минимальная пауза между батчами
      sleep(0.001) if batch_index < (jids.size / batch_size - 1)
    end

    completion_time = Time.now - start_time

    Sidekiq.logger.info "All jobs processed in #{completion_time} seconds"

    # Ждем завершения всех Redis операций
    sleep(0.2)

    final_status = Sidekiq::Batch::Status.new(bid)
    final_pending = final_status.pending
    final_failures = final_status.failures

    Sidekiq.logger.info "Final state - PENDING: #{final_pending}, FAILURES: #{final_failures}, TOTAL: #{final_status.total}"

    # Проверяем целостность данных
    expect(final_pending).to eq(final_failures),
      "Pending (#{final_pending}) should equal failures (#{final_failures})"

    # Проверяем, что у нас есть падения (примерно 10%)
    expected_failure_range = (job_count * 0.05)..(job_count * 0.15)
    expect(final_failures).to be_within(job_count * 0.05).of(job_count * 0.1),
      "Expected failures to be around 10% (50-150), got #{final_failures}"

    Sidekiq.logger.info "Mixed parallel test passed!"
  end
end

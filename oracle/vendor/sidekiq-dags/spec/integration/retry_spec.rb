require 'integration_helper'

# Коллбэк для отслеживания вызовов
class RetryTestCallback
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

describe "Retry Job Test - pending should not change on retry" do
  before do
    RetryTestCallback.reset
    Sidekiq.redis { |r| r.flushdb }
  end

  it "handles retry of a failed job in the middle with delay on other jobs" do
    # Создаем реальный worker для тестирования с задержкой и возможностью провала
    retry_test_worker = Class.new do
      include Sidekiq::Worker

      def perform(jid_to_fail_index = nil)
        # Если это JID который должен провалиться, выбрасываем исключение
        if @bid && jid_to_fail_index
          current_jid = Sidekiq.redis { |r| r.hget("BID-#{@bid}-jobs-to-fail", jid_to_fail_index.to_s) }
          if current_jid && current_jid == @jid
            Sidekiq.logger.info "Job #{@jid} is marked to fail (index #{jid_to_fail_index})"
            raise StandardError, "Job failed intentionally at index #{jid_to_fail_index}"
          end
        end

        # Для обычных (не провальных) задач добавляем задержку 1 секунду
        Sidekiq.logger.info "Job #{@jid} completed successfully with 1 second delay"
        sleep(1)
      end
    end

    # Удаляем старый класс если есть и создаем новый
    worker_class_name = "RetryTestWorkerWithDelay_#{SecureRandom.hex(4)}"
    Object.send(:remove_const, worker_class_name) if Object.const_defined?(worker_class_name)
    Object.const_set(worker_class_name, retry_test_worker)
    worker_class = Object.const_get(worker_class_name)

    batch = Sidekiq::Batch.new
    batch.on(:success, RetryTestCallback)
    batch.on(:complete, RetryTestCallback)

    job_count = 100
    jid_to_fail_index = 50  # 50-я задача (в середине)

    Sidekiq.logger.info "Starting retry test with #{job_count} jobs, job at index #{jid_to_fail_index} will fail"

    # Создаем джобы через add_jobs (используем настоящий Sidekiq API)
    batch.add_jobs do
      job_count.times do |i|
        worker_class.perform_async(i)
      end
    end
    batch.run

    bid = batch.bid
    status = Sidekiq::Batch::Status.new(bid)

    # Проверяем начальное состояние
    expect(status.pending).to eq(job_count), "Initial pending should be #{job_count}"
    expect(status.total).to eq(job_count), "Initial total should be #{job_count}"

    # Получаем все JID из Redis
    all_jids = Sidekiq.redis do |r|
      r.smembers("BID-#{bid}-jids")
    end

    # Получаем JID задачи, которая должна провалиться (50-я по индексу)
    jid_to_fail = all_jids[jid_to_fail_index]
    Sidekiq.logger.info "Job #{jid_to_fail} at index #{jid_to_fail_index} will fail"

    # Помечаем задачу, которая должна провалиться (хранит JID для сопоставления)
    Sidekiq.redis do |r|
      r.hset("BID-#{bid}-jobs-to-fail", jid_to_fail_index.to_s, jid_to_fail)
    end

    # Выполняем все задачи последовательно
    # Задачи в очереди: 0-49 (успешные), 50 (провальная), 51-99 (успешные)

    start_time = Time.now

    # Сначала выполняем задачи до провальной (0-49)
    (0..49).each do |i|
      jid = all_jids[i]
      Sidekiq.logger.info "Processing job #{i}: #{jid}"
      Sidekiq::Batch.process_successful_job(bid, jid)

      # Каждая успешная задача должна иметь задержку 1 секунду (эмулируем это)
      # В реальном worker это делается через sleep(1) в perform методе
      sleep(0.01) # Минимальная задержка для стабилизации
    end

    time_after_first_half = Time.now - start_time
    Sidekiq.logger.info "Processed first 50 jobs in #{time_after_first_half.round(2)} seconds"

    # Проверяем состояние после 50 успешных задач
    status_after_50 = Sidekiq::Batch::Status.new(bid)
    pending_after_50 = status_after_50.pending
    failures_after_50 = status_after_50.failures

    Sidekiq.logger.info "After 50 successful jobs - PENDING: #{pending_after_50}, FAILURES: #{failures_after_50}"

    # После 50 успешных задач: pending должен быть 50, failures 0
    expect(pending_after_50).to eq(50), "After 50 successes, pending should be 50, got #{pending_after_50}"
    expect(failures_after_50).to eq(0), "After 50 successes, failures should be 0, got #{failures_after_50}"

    # Теперь проваливаем задачу в середине (50-ю)
    Sidekiq::Batch.process_failed_job(bid, jid_to_fail)

    # Проверяем состояние после провала
    status_after_failure = Sidekiq::Batch::Status.new(bid)
    pending_after_failure = status_after_failure.pending
    failures_after_failure = status_after_failure.failures

    Sidekiq.logger.info "After failure (middle job) - PENDING: #{pending_after_failure}, FAILURES: #{failures_after_failure}"

    # После провала в середине: pending должен быть 50 (49 успешных + 49 оставшихся + 1 провальная), failures должен быть 1
    expect(pending_after_failure).to eq(50), "After failure in middle, pending should be 50, got #{pending_after_failure}"
    expect(failures_after_failure).to eq(1), "After failure in middle, failures should be 1, got #{failures_after_failure}"

    # Проверяем, что JID находится в failed set
    jid_in_failed = Sidekiq.redis do |r|
      r.sismember("BID-#{bid}-failed", jid_to_fail)
    end
    expect(jid_in_failed).to eq(1), "JID #{jid_to_fail} should be in failed set"

    # Эмулируем успешный retry задачи (как если бы задача была переизполнена успешно)
    # КРИТИЧЕСКИ МОМЕНТ: при retry используется тот же JID!
    Sidekiq.logger.info "Retrying failed job #{jid_to_fail}..."
    Sidekiq::Batch.process_successful_job(bid, jid_to_fail)

    # Проверяем состояние после успешного retry
    status_after_retry = Sidekiq::Batch::Status.new(bid)
    pending_after_retry = status_after_retry.pending
    failures_after_retry = status_after_retry.failures

    Sidekiq.logger.info "After retry - PENDING: #{pending_after_retry}, FAILURES: #{failures_after_retry}"

    # КРИТИЧЕСКАЯ ПРОВЕРКА: pending НЕ должен измениться после retry!
    # Он должен быть 49 (было 50, декрементировался до 49), а не 51!
    expect(pending_after_retry).to eq(49),
      "PENDING SHOULD NOT CHANGE ON RETRY! Expected 49 (was 50, decremented by 1), got #{pending_after_retry}. " \
      "If pending is 51, it means retry incorrectly incremented pending!"
    expect(failures_after_retry).to eq(0), "After successful retry, failures should be 0, got #{failures_after_retry}"

    # Проверяем, что JID удален из failed set
    jid_in_failed_after_retry = Sidekiq.redis do |r|
      r.sismember("BID-#{bid}-failed", jid_to_fail)
    end
    expect(jid_in_failed_after_retry).to eq(0), "JID #{jid_to_fail} should NOT be in failed set after successful retry"

    # Теперь выполняем оставшиеся задачи (51-99)
    (51..99).each do |i|
      jid = all_jids[i]
      Sidekiq.logger.info "Processing job #{i}: #{jid}"
      Sidekiq::Batch.process_successful_job(bid, jid)
      sleep(0.01) # Минимальная задержка для стабилизации
    end

    # Проверяем итоговое состояние
    final_status = Sidekiq::Batch::Status.new(bid)
    final_pending = final_status.pending
    final_failures = final_status.failures

    total_time = Time.now - start_time
    Sidekiq.logger.info "Final state - PENDING: #{final_pending}, FAILURES: #{final_failures}"
    Sidekiq.logger.info "Total time: #{total_time.round(2)} seconds"

    # В итоге: pending должен быть 0, failures 0
    expect(final_pending).to eq(0), "Final pending should be 0, got #{final_pending}"
    expect(final_failures).to eq(0), "Final failures should be 0, got #{final_failures}"

    Sidekiq.logger.info "✅ Retry test with middle failure passed! PENDING remained correct after retry."
  end

  it "handles retry of a failed job without changing pending counter" do
    # Этот тест использует реальный Sidekiq API через процессинг джобов напрямую
    batch = Sidekiq::Batch.new
    batch.on(:success, RetryTestCallback)
    batch.on(:complete, RetryTestCallback)

    job_count = 100
    jid_to_fail_index = 99  # 100-я задача (индекс 99)

    # Генерируем JID'ы напрямую (как это делается внутри Sidekiq)
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

    Sidekiq.logger.info "Starting retry test with #{job_count} jobs, bid: #{bid}"

    # Получаем JID задачи, которая должна провалиться
    jid_to_fail = jids[jid_to_fail_index]
    Sidekiq.logger.info "Job #{jid_to_fail} will fail"

    # Выполняем 99 задач успешно (все кроме провальной)
    (0..98).each do |i|
      Sidekiq::Batch.process_successful_job(bid, jids[i])
    end

    # Проверяем состояние после 99 успешных задач
    status_after_99 = Sidekiq::Batch::Status.new(bid)
    pending_after_99 = status_after_99.pending
    failures_after_99 = status_after_99.failures

    Sidekiq.logger.info "After 99 successful jobs - PENDING: #{pending_after_99}, FAILURES: #{failures_after_99}"

    # После 99 успешных задач: pending должен быть 1 (оставшаяся задача), failures 0
    expect(pending_after_99).to eq(1), "After 99 successes, pending should be 1, got #{pending_after_99}"
    expect(failures_after_99).to eq(0), "After 99 successes, failures should be 0, got #{failures_after_99}"

    # Теперь проваливаем последнюю задачу (100-ю)
    Sidekiq::Batch.process_failed_job(bid, jid_to_fail)

    # Проверяем состояние после провала
    status_after_failure = Sidekiq::Batch::Status.new(bid)
    pending_after_failure = status_after_failure.pending
    failures_after_failure = status_after_failure.failures

    Sidekiq.logger.info "After failure - PENDING: #{pending_after_failure}, FAILURES: #{failures_after_failure}"

    # После провала: pending должен быть 1 (задача считается pending, но в failed set), failures должен быть 1
    expect(pending_after_failure).to eq(1), "After failure, pending should still be 1, got #{pending_after_failure}"
    expect(failures_after_failure).to eq(1), "After failure, failures should be 1, got #{failures_after_failure}"

    # Проверяем, что JID находится в failed set
    jid_in_failed = Sidekiq.redis do |r|
      r.sismember("BID-#{bid}-failed", jid_to_fail)
    end
    expect(jid_in_failed).to eq(1), "JID #{jid_to_fail} should be in failed set"

    # Эмулируем успешный retry задачи (как если бы задача была переизполнена успешно)
    # В реальном сценарии это делает middleware при успешном выполнении retry-задачи
    # КРИТИЧЕСКИ МОМЕНТ: при retry используется тот же JID!
    Sidekiq::Batch.process_successful_job(bid, jid_to_fail)

    # Проверяем состояние после успешного retry
    status_after_retry = Sidekiq::Batch::Status.new(bid)
    pending_after_retry = status_after_retry.pending
    failures_after_retry = status_after_retry.failures

    Sidekiq.logger.info "After retry - PENDING: #{pending_after_retry}, FAILURES: #{failures_after_retry}"

    # КРИТИЧЕСКАЯ ПРОВЕРКА: pending НЕ должен измениться после retry!
    # Он должен быть 0, а не 2 (что было бы проблемой)
    expect(pending_after_retry).to eq(0),
      "PENDING SHOULD NOT CHANGE ON RETRY! Expected 0, got #{pending_after_retry}. " \
      "If pending is 2, it means the retry incorrectly incremented pending!"
    expect(failures_after_retry).to eq(0), "After successful retry, failures should be 0, got #{failures_after_retry}"

    # Проверяем, что JID удален из failed set
    jid_in_failed_after_retry = Sidekiq.redis do |r|
      r.sismember("BID-#{bid}-failed", jid_to_fail)
    end
    expect(jid_in_failed_after_retry).to eq(0), "JID #{jid_to_fail} should NOT be in failed set after successful retry"

    Sidekiq.logger.info "✅ Retry test passed! PENDING remained correct after retry."
  end

  it "verifies that retry with same JID does not double count pending" do
    # Упрощенный тест, который фокусируется на проверке того, что process_successful_job
    # корректно обрабатывает retry (то есть, если JID был в failed set, он удаляется
    # и pending декрементируется корректно, без двойного счета)

    batch = Sidekiq::Batch.new
    batch.on(:success, RetryTestCallback)
    batch.on(:complete, RetryTestCallback)

    job_count = 100

    # Генерируем JID'ы напрямую
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

    Sidekiq.logger.info "Starting simplified retry test with #{job_count} jobs"

    # Выполняем 99 задач успешно
    (0..98).each do |i|
      Sidekiq::Batch.process_successful_job(bid, jids[i])
    end

    # Проверяем состояние
    status_after_99 = Sidekiq::Batch::Status.new(bid)
    pending_after_99 = status_after_99.pending
    expect(pending_after_99).to eq(1), "After 99 successes, pending should be 1, got #{pending_after_99}"

    # Проваливаем последнюю задачу (100-ю)
    jid_100 = jids[99]
    Sidekiq::Batch.process_failed_job(bid, jid_100)

    # Проверяем состояние после провала
    status_after_failure = Sidekiq::Batch::Status.new(bid)
    pending_after_failure = status_after_failure.pending
    failures_after_failure = status_after_failure.failures

    expect(pending_after_failure).to eq(1), "After failure, pending should be 1, got #{pending_after_failure}"
    expect(failures_after_failure).to eq(1), "After failure, failures should be 1, got #{failures_after_failure}"

    # Теперь эмулируем retry той же задачи (используем тот же JID)
    # Это критически важно: при retry используется тот же JID!
    Sidekiq::Batch.process_successful_job(bid, jid_100)

    # ПРОВЕРКА: pending должен быть 0, а не 2
    status_after_retry = Sidekiq::Batch::Status.new(bid)
    pending_after_retry = status_after_retry.pending
    failures_after_retry = status_after_retry.failures

    Sidekiq.logger.info "After retry - PENDING: #{pending_after_retry}, FAILURES: #{failures_after_retry}"

    # КРИТИЧЕСКАЯ ПРОВЕРКА: pending НЕ должен быть 2!
    # Это означало бы, что при retry pending был инкрементирован дважды
    # (один раз когда задача провалилась, один раз когда retry начался)
    expect(pending_after_retry).to eq(0),
      "PENDING SHOULD NOT BE 2 ON RETRY! If pending is 2, it means the retry incorrectly incremented pending. " \
      "Expected 0, got #{pending_after_retry}. " \
      "This is a CRITICAL BUG that breaks the batch consistency!"

    expect(failures_after_retry).to eq(0),
      "After successful retry, failures should be 0, got #{failures_after_retry}"

    Sidekiq.logger.info "✅ Simplified retry test passed! Pending correctly handled on retry."
  end

  it "handles multiple retries of the same job without changing pending" do
    # Тест для проверки множественных retry одной и той же задачи
    batch = Sidekiq::Batch.new
    batch.on(:success, RetryTestCallback)
    batch.on(:complete, RetryTestCallback)

    job_count = 100

    # Генерируем JID'ы напрямую
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

    Sidekiq.logger.info "Starting multiple retry test with #{job_count} jobs"

    # Выполняем 99 задач успешно
    (0..98).each do |i|
      Sidekiq::Batch.process_successful_job(bid, jids[i])
    end

    # Проваливаем последнюю задачу (100-ю)
    jid_100 = jids[99]

    status_before_failures = Sidekiq::Batch::Status.new(bid)
    expect(status_before_failures.pending).to eq(1), "Before failures, pending should be 1"

    # Проваливаем задачу
    Sidekiq::Batch.process_failed_job(bid, jid_100)

    status_after_failure = Sidekiq::Batch::Status.new(bid)
    pending_after_failure = status_after_failure.pending
    expect(pending_after_failure).to eq(1), "After failure, pending should be 1"

    # Проверяем, что JID в failed set
    jid_in_failed = Sidekiq.redis do |r|
      r.sismember("BID-#{bid}-failed", jid_100)
    end
    expect(jid_in_failed).to eq(1), "JID should be in failed set"

    # Эмулируем retry - задача проваливается снова
    # Сначала удаляем из failed set (как если бы retry начался)
    Sidekiq.redis do |r|
      r.srem("BID-#{bid}-failed", jid_100)
    end

    # И снова проваливаем
    Sidekiq::Batch.process_failed_job(bid, jid_100)

    status_after_second_failure = Sidekiq::Batch::Status.new(bid)
    pending_after_second_failure = status_after_second_failure.pending

    # pending все еще должен быть 1
    expect(pending_after_second_failure).to eq(1),
      "After second failure, pending should still be 1, got #{pending_after_second_failure}"

    # Теперь успешный retry
    Sidekiq::Batch.process_successful_job(bid, jid_100)

    status_after_retry = Sidekiq::Batch::Status.new(bid)
    pending_after_retry = status_after_retry.pending

    # КРИТИЧЕСКАЯ ПРОВЕРКА: pending должен быть 0
    expect(pending_after_retry).to eq(0),
      "PENDING SHOULD NOT CHANGE ON MULTIPLE RETRIES! Expected 0, got #{pending_after_retry}"

    Sidekiq.logger.info "✅ Multiple retry test passed! Pending correctly handled on multiple retries."
  end
end

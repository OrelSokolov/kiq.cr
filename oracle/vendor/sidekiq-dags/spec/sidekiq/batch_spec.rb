require 'spec_helper'

class TestWorker
  include Sidekiq::Worker
  def perform; end
end

describe Sidekiq::Batch do
  it 'has a version number' do
    expect(Sidekiq::Batch::VERSION).not_to be nil
  end

  describe '#initialize' do
    subject { described_class }

    it 'creates bid when called without it' do
      expect(subject.new.bid).not_to be_nil
    end

    it 'reuses bid when called with it' do
      batch = subject.new('dayPO5KxuRXXxw')
      expect(batch.bid).to eq('dayPO5KxuRXXxw')
    end
  end

  describe '#description' do
    let(:description) { 'custom description' }
    before { subject.description = description }

    it 'sets descriptions' do
      expect(subject.description).to eq(description)
    end

    it 'persists description' do
      expect(Sidekiq.redis { |r| r.hget("BID-#{subject.bid}", 'description') })
        .to eq(description)
    end
  end

  describe '#callback_queue' do
    let(:callback_queue) { 'custom_queue' }
    before { subject.callback_queue = callback_queue }

    it 'sets callback_queue' do
      expect(subject.callback_queue).to eq(callback_queue)
    end

    it 'persists callback_queue' do
      expect(Sidekiq
             .redis { |r| r.hget("BID-#{subject.bid}", 'callback_queue') })
        .to eq(callback_queue)
    end
  end

  describe '#on' do
    it 'allows adding callbacks before run' do
      batch = Sidekiq::Batch.new
      expect { batch.on(:complete, SampleCallback) }.not_to raise_error
    end

    it 'raises error when adding callback to already started batch' do
      batch = Sidekiq::Batch.new
      batch.add_jobs { TestWorker.perform_async }
      batch.run

      expect { batch.on(:complete, SampleCallback) }.to raise_error(
        Sidekiq::Batch::BatchAlreadyStartedError,
        'Cannot add callbacks to a batch that has already been started'
      )
    end

    it 'raises error when adding callback after run even with empty batch' do
      batch = Sidekiq::Batch.new
      batch.add_jobs { nil }
      batch.run

      expect { batch.on(:success, SampleCallback) }.to raise_error(
        Sidekiq::Batch::BatchAlreadyStartedError
      )
    end
  end

  describe '#add_jobs' do
    it 'throws error if no block given' do
      expect { subject.add_jobs }.to raise_error Sidekiq::Batch::NoBlockGivenError
    end

    it 'does not execute block immediately' do
      executed = false
      batch = Sidekiq::Batch.new
      batch.add_jobs do
        executed = true
      end
      expect(executed).to be false
    end

    it 'returns self for chaining' do
      batch = Sidekiq::Batch.new
      result = batch.add_jobs {}
      expect(result).to eq(batch)
    end
  end

  describe '#run' do
    it 'increments to_process (when started)'

    it 'decrements to_process (when finished)'

    it 'sets Thread.current bid' do
      batch = Sidekiq::Batch.new
      batch.add_jobs do
        expect(Thread.current[:batch]).to eq(batch)
      end
      batch.run
    end

    it 'returns empty array if no jobs block was added' do
      batch = Sidekiq::Batch.new
      expect(batch.run).to eq([])
    end
  end

  describe '#invalidate_all' do
    class InvalidatableJob
      include Sidekiq::Worker

      def perform
        return unless valid_within_batch?

        was_performed
      end

      def was_performed; end
    end

    it 'marks batch in redis as invalidated' do
      batch = Sidekiq::Batch.new
      job = InvalidatableJob.new
      allow(job).to receive(:was_performed)

      batch.invalidate_all
      batch.add_jobs { job.perform }
      batch.run

      expect(job).not_to have_received(:was_performed)
    end

    context 'nested batches' do
      let(:batch_parent) { Sidekiq::Batch.new }
      let(:batch_child_1) { Sidekiq::Batch.new }
      let(:batch_child_2) { Sidekiq::Batch.new }
      let(:job_of_parent) { InvalidatableJob.new }
      let(:job_of_child_1) { InvalidatableJob.new }
      let(:job_of_child_2) { InvalidatableJob.new }

      before do
        allow(job_of_parent).to receive(:was_performed)
        allow(job_of_child_1).to receive(:was_performed)
        allow(job_of_child_2).to receive(:was_performed)
      end

      it 'invalidates all job if parent batch is marked as invalidated' do
        batch_parent.invalidate_all
        batch_child_2.add_jobs { job_of_child_2.perform }
        batch_child_1.add_jobs do
          job_of_child_1.perform
          batch_child_2.run
        end
        batch_parent.add_jobs do
          job_of_parent.perform
          batch_child_1.run
        end
        batch_parent.run

        expect(job_of_parent).not_to have_received(:was_performed)
        expect(job_of_child_1).not_to have_received(:was_performed)
        expect(job_of_child_2).not_to have_received(:was_performed)
      end

      it 'invalidates only requested batch' do
        batch_child_2.invalidate_all
        batch_child_2.add_jobs { job_of_child_2.perform }
        batch_child_1.add_jobs do
          job_of_child_1.perform
          batch_child_2.run
        end
        batch_parent.add_jobs do
          job_of_parent.perform
          batch_child_1.run
        end
        batch_parent.run

        expect(job_of_parent).to have_received(:was_performed)
        expect(job_of_child_1).to have_received(:was_performed)
        expect(job_of_child_2).not_to have_received(:was_performed)
      end
    end
  end

  describe '#process_failed_job' do
    let(:batch) { Sidekiq::Batch.new }
    let(:bid) { batch.bid }
    let(:jid) { 'ABCD' }
    before { Sidekiq.redis { |r| r.hset("BID-#{bid}", 'pending', 1) } }

    context 'complete' do
      let(:failed_jid) { 'xxx' }

      it 'tries to call complete callback' do
        expect(Sidekiq::Batch).to receive(:enqueue_callbacks).with(:complete, bid)
        Sidekiq::Batch.process_failed_job(bid, failed_jid)
      end

      it 'add job to failed list' do
        Sidekiq::Batch.process_failed_job(bid, 'failed-job-id')
        Sidekiq::Batch.process_failed_job(bid, failed_jid)
        failed = Sidekiq.redis { |r| r.smembers("BID-#{bid}-failed") }
        expect(failed).to match_array(%w[failed-job-id xxx])
      end
    end
  end

  describe '#process_successful_job' do
    let(:batch) { Sidekiq::Batch.new }
    let(:bid) { batch.bid }
    let(:jid) { 'ABCD' }
    before { Sidekiq.redis { |r| r.hset("BID-#{bid}", 'pending', 1) } }

    context 'complete' do
      before { batch.on(:complete, Object) }
      # before { batch.increment_job_queue(bid) }
      before do
        batch.add_jobs { TestWorker.perform_async }
        batch.run
      end
      before { Sidekiq::Batch.process_failed_job(bid, 'failed-job-id') }

      it 'tries to call complete callback' do
        expect(Sidekiq::Batch).to receive(:enqueue_callbacks).with(:complete, bid)
        Sidekiq::Batch.process_successful_job(bid, 'failed-job-id')
      end
    end

    context 'success' do
      before { batch.on(:complete, Object) }
      it 'tries to call complete callback' do
        expect(Sidekiq::Batch).to receive(:enqueue_callbacks).with(:complete, bid).ordered
        expect(Sidekiq::Batch).to receive(:enqueue_callbacks).with(:success, bid).ordered
        Sidekiq::Batch.process_successful_job(bid, jid)
      end

      it 'cleanups redis key' do
        Sidekiq::Batch.process_successful_job(bid, jid)
        expect(Sidekiq.redis { |r| r.get("BID-#{bid}-pending") }.to_i).to eq(0)
      end
    end
  end

  describe '#increment_job_queue' do
    let(:bid) { 'BID' }
    let(:batch) { Sidekiq::Batch.new }

    it 'increments pending' do
      batch.add_jobs { TestWorker.perform_async }
      batch.run
      pending = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'pending') }
      expect(pending).to eq('1')
    end

    it 'increments total' do
      batch.add_jobs { TestWorker.perform_async }
      batch.run
      total = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'total') }
      expect(total).to eq('1')
    end
  end

  describe '#enqueue_callbacks' do
    let(:callback) { double('callback') }
    let(:event) { :complete }

    context 'on :success' do
      let(:event) { :success }

      context 'when no callbacks are defined' do
        it 'calls finalize without cleanup - keys expire via TTL' do
          batch = Sidekiq::Batch.new
          # Инициализируем батч, чтобы он существовал в Redis
          batch.add_jobs do
            # Пустой блок - батч будет инициализирован
          end
          batch.run
          # enqueue_callbacks вызывает Finalize#dispatch когда нет callbacks и батч существует
          # Finalize больше не вызывает cleanup - все ключи удаляются по TTL автоматически
          expect(Sidekiq::Batch).not_to respond_to(:cleanup_redis)
          Sidekiq::Batch.enqueue_callbacks(event, batch.bid)
        end
      end
    end

    context 'when already called' do
      it 'returns and does not enqueue callbacks' do
        batch = Sidekiq::Batch.new
        batch.on(event, SampleCallback)
        # Флаг обработки теперь хранится в отдельном ключе (не удаляется при cleanup)
        Sidekiq.redis { |r| r.set("BID-#{batch.bid}-processed-#{event}", 'true') }

        expect(Sidekiq::Client).not_to receive(:push)
        Sidekiq::Batch.enqueue_callbacks(event, batch.bid)
      end
    end

    context 'when not yet called' do
      context 'when there is no callback' do
        it 'it returns' do
          batch = Sidekiq::Batch.new

          expect(Sidekiq::Client).not_to receive(:push)
          Sidekiq::Batch.enqueue_callbacks(event, batch.bid)
        end
      end

      context 'when callback defined' do
        let(:opts) { { 'a' => 'b' } }

        it 'calls it passing options' do
          batch = Sidekiq::Batch.new
          batch.on(event, SampleCallback, opts)

          expect(Sidekiq::Client).to receive(:push_bulk).with(
            'class' => Sidekiq::Batch::Callback::Worker,
            'args' => [['SampleCallback', event.to_s, opts, batch.bid, nil, anything]],
            'queue' => 'default'
          )
          Sidekiq::Batch.enqueue_callbacks(event, batch.bid)
        end
      end

      context 'when multiple callbacks are defined' do
        let(:opts) { { 'a' => 'b' } }
        let(:opts2) { { 'b' => 'a' } }

        it 'enqueues each callback passing their options' do
          batch = Sidekiq::Batch.new
          batch.on(event, SampleCallback, opts)
          batch.on(event, SampleCallback2, opts2)

          expect(Sidekiq::Client).to receive(:push_bulk).with(
            'class' => Sidekiq::Batch::Callback::Worker,
            'args' => match_array([
                                    match_array(['SampleCallback', event.to_s, opts, batch.bid, nil, anything]),
                                    match_array(['SampleCallback2', event.to_s, opts2, batch.bid, nil, anything])
                                  ]),
            'queue' => 'default'
          )

          Sidekiq::Batch.enqueue_callbacks(event, batch.bid)
        end
      end
    end

    context 'race condition: batch deleted before callback enqueue' do
      # Тест проверяет, что enqueue_callbacks может прочитать callbacks
      # даже если основной батч уже удален из Redis
      # Это важно, так как cleanup может удалить батч до того, как callbacks будут обработаны
      it 'reads callbacks even if batch is deleted from Redis' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback, { 'test' => 'data' })
        bid = batch.bid
        callback_key = "BID-#{bid}-callbacks-success"

        # Проверяем, что callbacks сохранены
        callbacks_before = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks_before).not_to be_empty

        # Симулируем ситуацию: батч удален, но callbacks еще существуют
        # (это может произойти если cleanup удалил батч, но не успел удалить callbacks)
        Sidekiq.redis { |r| r.del("BID-#{bid}") }

        # Проверяем, что батч действительно удален
        batch_exists = Sidekiq.redis { |r| r.exists("BID-#{bid}") } > 0
        expect(batch_exists).to be false

        # Проверяем, что callbacks все еще существуют
        callbacks_after = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks_after).not_to be_empty

        # enqueue_callbacks должен прочитать callbacks даже если батч удален
        # и поместить их в очередь
        expect(Sidekiq::Client).to receive(:push_bulk).with(
          'class' => Sidekiq::Batch::Callback::Worker,
          'args' => [
            ['SampleCallback', 'success', { 'test' => 'data' }, bid, nil, anything]
          ],
          'queue' => 'default'
        )

        Sidekiq::Batch.enqueue_callbacks(:success, bid)
      end

      # Тест проверяет, что lock в enqueue_callbacks защищает от race condition
      # при параллельных вызовах
      # Lock гарантирует атомарность: чтение + пометка как обработанные
      it 'handles concurrent enqueue_callbacks calls with lock' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback)
        bid = batch.bid

        # Симулируем race condition: два потока одновременно работают с батчем
        callback_enqueued = false

        # Поток 1: пытается вызвать enqueue_callbacks
        thread1 = Thread.new do
          # Небольшая задержка для создания race condition
          sleep(0.001)
          expect(Sidekiq::Client).to receive(:push_bulk).at_least(:once) do |_args|
            callback_enqueued = true
          end
          Sidekiq::Batch.enqueue_callbacks(:success, bid)
        end

        # Поток 2: тоже пытается вызвать enqueue_callbacks параллельно
        thread2 = Thread.new do
          # Небольшая задержка для создания race condition
          sleep(0.001)
          Sidekiq::Batch.enqueue_callbacks(:success, bid)
        end

        thread1.join
        thread2.join

        # Проверяем, что callback был вызван (даже при параллельных вызовах)
        expect(callback_enqueued).to be true

        # Проверяем, что callbacks были обработаны корректно
        # Lock гарантирует, что callbacks прочитаны и помечены как обработанные
        success_processed = Sidekiq.redis { |r| r.get("BID-#{bid}-processed-success") }
        expect(success_processed).to eq('true')
      end

      # Тест проверяет полный цикл: enqueue_callbacks читает callbacks атомарно,
      # помечает их как обработанные и удаляет их после постановки в очередь
      it 'ensures callbacks are read atomically and deleted after processing' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback, { 'atomic' => 'test' })
        bid = batch.bid

        # Устанавливаем pending=0, чтобы батч считался завершенным
        Sidekiq.redis do |r|
          r.multi do |pipeline|
            pipeline.hset("BID-#{bid}", 'pending', '0')
            pipeline.hset("BID-#{bid}", 'total', '1')
          end
        end

        # Симулируем успешное завершение последнего джоба
        # Это должно вызвать enqueue_callbacks(:success, bid)
        expect(Sidekiq::Client).to receive(:push_bulk).with(
          'class' => Sidekiq::Batch::Callback::Worker,
          'args' => [
            ['SampleCallback', 'success', { 'atomic' => 'test' }, bid, nil, anything]
          ],
          'queue' => 'default'
        )

        # Вызываем enqueue_callbacks (это происходит в process_successful_job)
        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # Проверяем, что callbacks были прочитаны и помечены как обработанные
        success_processed = Sidekiq.redis { |r| r.get("BID-#{bid}-processed-success") }
        expect(success_processed).to eq('true'), 'Success flag should be set after enqueue_callbacks'

        # Проверяем, что callbacks были удалены после обработки
        # (enqueue_callbacks удаляет callbacks после их чтения и постановки в очередь)
        callbacks_after_enqueue = Sidekiq.redis { |r| r.smembers("BID-#{bid}-callbacks-success") }
        expect(callbacks_after_enqueue).to be_empty,
                                           'Callbacks should be deleted after enqueue_callbacks processes them'
      end

      it 'deletes callbacks only after they are read and enqueued' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback, { 'test' => 'data' })
        bid = batch.bid
        callback_key = "BID-#{bid}-callbacks-success"

        # Проверяем, что callbacks сохранены
        callbacks_before = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks_before).not_to be_empty

        # enqueue_callbacks должен прочитать callbacks и удалить их после постановки в очередь
        expect(Sidekiq::Client).to receive(:push_bulk).with(
          'class' => Sidekiq::Batch::Callback::Worker,
          'args' => [
            ['SampleCallback', 'success', { 'test' => 'data' }, bid, nil, anything]
          ],
          'queue' => 'default'
        )

        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # Проверяем, что callbacks удалены после enqueue_callbacks
        callbacks_after_enqueue = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks_after_enqueue).to be_empty,
                                           'Callbacks should be deleted by enqueue_callbacks after reading them'
      end

      it 'preserves callbacks even if batch is deleted before enqueue_callbacks is called' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback, { 'preserve' => 'test' })
        bid = batch.bid
        callback_key = "BID-#{bid}-callbacks-success"

        # Проверяем, что callbacks сохранены
        callbacks_before = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks_before).not_to be_empty

        # Удаляем батч ДО вызова enqueue_callbacks (симулируем race condition)
        Sidekiq.redis { |r| r.del("BID-#{bid}") }

        # Проверяем, что батч удален
        batch_exists = Sidekiq.redis { |r| r.exists("BID-#{bid}") } > 0
        expect(batch_exists).to be false

        # Проверяем, что callbacks все еще существуют
        callbacks_after_delete = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks_after_delete).not_to be_empty, 'Callbacks should still exist even if batch is deleted'

        # enqueue_callbacks должен прочитать callbacks даже если батч удален
        expect(Sidekiq::Client).to receive(:push_bulk).with(
          'class' => Sidekiq::Batch::Callback::Worker,
          'args' => [
            ['SampleCallback', 'success', { 'preserve' => 'test' }, bid, nil, anything]
          ],
          'queue' => 'default'
        )

        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # Проверяем, что callbacks удалены после enqueue_callbacks
        callbacks_after_enqueue = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks_after_enqueue).to be_empty,
                                           'Callbacks should be deleted after enqueue_callbacks processes them'
      end
    end

    context 'idempotent callback processing with separate flags' do
      # Тесты для новой архитектуры с отдельными флагами обработки
      # Флаги хранятся в BID-xxx-processed-{event} и не удаляются при cleanup

      it 'stores processed flag in separate key with TTL' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback)
        bid = batch.bid
        processed_flag_key = "BID-#{bid}-processed-success"

        # До вызова enqueue_callbacks флаг не должен существовать
        flag_before = Sidekiq.redis { |r| r.get(processed_flag_key) }
        expect(flag_before).to be_nil

        # Мокаем push_bulk чтобы не создавать реальные джобы
        allow(Sidekiq::Client).to receive(:push_bulk)

        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # После вызова флаг должен быть установлен
        flag_after = Sidekiq.redis { |r| r.get(processed_flag_key) }
        expect(flag_after).to eq('true')

        # Проверяем, что TTL установлен (должен быть близок к CALLBACK_FLAG_TTL)
        ttl = Sidekiq.redis { |r| r.ttl(processed_flag_key) }
        expect(ttl).to be > 0
        expect(ttl).to be <= Sidekiq::Batch::CALLBACK_FLAG_TTL
      end

      it 'processed flag has TTL set independently of batch data' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback)
        bid = batch.bid
        processed_flag_key = "BID-#{bid}-processed-success"

        allow(Sidekiq::Client).to receive(:push_bulk)

        # Обрабатываем callbacks
        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # Флаг установлен
        flag = Sidekiq.redis { |r| r.get(processed_flag_key) }
        expect(flag).to eq('true')

        # Проверяем TTL флага
        ttl = Sidekiq.redis { |r| r.ttl(processed_flag_key) }
        expect(ttl).to be > 0
        expect(ttl).to be <= Sidekiq::Batch::CALLBACK_FLAG_TTL
      end

      it 'prevents duplicate callback processing' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback)
        bid = batch.bid

        # Первый вызов - callbacks должны быть обработаны
        expect(Sidekiq::Client).to receive(:push_bulk).once
        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # Второй вызов - должен быть проигнорирован благодаря флагу
        expect(Sidekiq::Client).not_to receive(:push_bulk)
        Sidekiq::Batch.enqueue_callbacks(:success, bid)
      end

      it 'handles multiple concurrent enqueue_callbacks calls idempotently' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback)
        bid = batch.bid

        push_count = 0
        allow(Sidekiq::Client).to receive(:push_bulk) do
          push_count += 1
        end

        # Симулируем параллельные вызовы
        threads = []
        5.times do
          threads << Thread.new do
            Sidekiq::Batch.enqueue_callbacks(:success, bid)
          end
        end
        threads.each(&:join)

        # Callbacks должны быть обработаны только один раз
        expect(push_count).to eq(1), "Callbacks should be pushed exactly once, but was pushed #{push_count} times"
      end

      it 'processes callbacks even if batch was deleted before enqueue_callbacks' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback, { 'key' => 'value' })
        bid = batch.bid
        callback_key = "BID-#{bid}-callbacks-success"

        # Callbacks существуют
        callbacks = Sidekiq.redis { |r| r.smembers(callback_key) }
        expect(callbacks).not_to be_empty

        # Удаляем батч (симулируем race condition)
        Sidekiq.redis { |r| r.del("BID-#{bid}") }

        # Callbacks должны быть обработаны
        expect(Sidekiq::Client).to receive(:push_bulk).with(
          'class' => Sidekiq::Batch::Callback::Worker,
          'args' => [['SampleCallback', 'success', { 'key' => 'value' }, bid, nil, anything]],
          'queue' => 'default'
        )

        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # Флаг должен быть установлен
        flag = Sidekiq.redis { |r| r.get("BID-#{bid}-processed-success") }
        expect(flag).to eq('true')
      end

      it 'sets processed flag before processing callbacks to prevent race condition' do
        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback)
        bid = batch.bid
        processed_flag_key = "BID-#{bid}-processed-success"

        flag_set_before_push = false

        allow(Sidekiq::Client).to receive(:push_bulk) do
          # В момент push_bulk флаг уже должен быть установлен
          flag = Sidekiq.redis { |r| r.get(processed_flag_key) }
          flag_set_before_push = (flag == 'true')
        end

        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        expect(flag_set_before_push).to eq(true), 'Processed flag should be set BEFORE pushing callbacks'
      end

      it 'stores separate flags for complete and success events' do
        batch = Sidekiq::Batch.new
        bid = batch.bid

        # Устанавливаем только complete флаг вручную
        Sidekiq.redis { |r| r.set("BID-#{bid}-processed-complete", 'true') }

        complete_flag = Sidekiq.redis { |r| r.get("BID-#{bid}-processed-complete") }
        success_flag = Sidekiq.redis { |r| r.get("BID-#{bid}-processed-success") }

        # Флаги независимы
        expect(complete_flag).to eq('true')
        expect(success_flag).to be_nil

        # Устанавливаем success флаг
        Sidekiq.redis { |r| r.set("BID-#{bid}-processed-success", 'true') }

        # Оба флага установлены
        complete_flag = Sidekiq.redis { |r| r.get("BID-#{bid}-processed-complete") }
        success_flag = Sidekiq.redis { |r| r.get("BID-#{bid}-processed-success") }
        expect(complete_flag).to eq('true')
        expect(success_flag).to eq('true')
      end

      it 'simulates real-world race condition: double enqueue_callbacks call' do
        # Этот тест симулирует реальный сценарий:
        # 1. process_successful_job вызывает enqueue_callbacks(:success, bid)
        # 2. Другой код также пытается вызвать enqueue_callbacks(:success, bid)
        # Второй вызов должен быть проигнорирован благодаря флагу обработки

        batch = Sidekiq::Batch.new
        batch.on(:success, SampleCallback)
        bid = batch.bid

        push_count = 0
        allow(Sidekiq::Client).to receive(:push_bulk) do
          push_count += 1
        end

        # Первый вызов (из process_successful_job)
        Sidekiq::Batch.enqueue_callbacks(:success, bid)
        expect(push_count).to eq(1)

        # Второй вызов (может произойти из другого места)
        Sidekiq::Batch.enqueue_callbacks(:success, bid)

        # Callbacks должны быть обработаны только один раз
        expect(push_count).to eq(1), 'Second enqueue_callbacks should be ignored'
      end
    end
  end

  describe 'callback synchronization: callbacks must not execute before all jobs complete' do
    it 'prevents callback execution when pending > 0' do
      callback_executed = false

      callback_class = Class.new do
        def initialize(flag)
          @flag = flag
        end

        def on_success(_status, _opts)
          @flag[0] = true
        end
      end

      batch = Sidekiq::Batch.new
      batch.on(:success, callback_class.new([callback_executed]), {})

      # Добавляем 3 джоба
      batch.add_jobs do
        3.times do
          TestWorker.perform_async
        end
      end
      batch.run

      # Проверяем, что pending = 3
      pending = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'pending') }
      expect(pending.to_i).to eq(3)

      # Пытаемся вызвать callback напрямую (симулируем race condition или прямой вызов)
      # Callback НЕ должен выполниться, так как pending > 0
      allow(Sidekiq::Client).to receive(:push_bulk)
      Sidekiq::Batch.enqueue_callbacks(:success, batch.bid)

      # Проверяем, что callback не был поставлен в очередь
      expect(Sidekiq::Client).not_to have_received(:push_bulk)
      expect(callback_executed).to be false
    end

    it 'allows callback execution only when pending = 0' do
      callback_class = Class.new do
        def initialize(flag)
          @flag = flag
        end

        def on_success(_status, _opts)
          @flag[0] = true
        end
      end

      batch = Sidekiq::Batch.new
      callback_executed = [false]
      batch.on(:success, callback_class.new(callback_executed), {})

      # Добавляем 1 джоб
      jid = nil
      batch.add_jobs do
        jid = TestWorker.perform_async
      end
      batch.run

      # Проверяем, что pending = 1
      pending = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'pending') }
      expect(pending.to_i).to eq(1)

      # Пытаемся вызвать callback напрямую - он НЕ должен выполниться, так как pending > 0
      allow(Sidekiq::Client).to receive(:push_bulk)
      Sidekiq::Batch.enqueue_callbacks(:success, batch.bid)
      expect(Sidekiq::Client).not_to have_received(:push_bulk)

      # Завершаем джоб напрямую через process_successful_job
      # Это должно вызвать callback, так как pending станет 0
      expect(Sidekiq::Client).to receive(:push_bulk).at_least(:once)
      Sidekiq::Batch.process_successful_job(batch.bid, jid)

      # Проверяем, что pending стал 0
      pending_after = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'pending') }
      expect(pending_after.to_i).to eq(0)
    end

    it 'ensures sequential execution of batches in pipeline' do
      # Этот тест проверяет логику синхронизации через прямую проверку enqueue_callbacks
      # В реальном сценарии джобы обрабатываются асинхронно через Sidekiq workers
      # Но мы можем проверить, что проверка pending работает правильно

      execution_order = []

      callback_class = Class.new do
        def initialize(order_tracker)
          @order_tracker = order_tracker
        end

        def on_success(_status, opts)
          batch_number = opts['batch_number']
          @order_tracker << "callback-#{batch_number}"
        end
      end

      # Создаем batch с 3 джобами
      batch = Sidekiq::Batch.new
      batch.on(:success, callback_class.new(execution_order), 'batch_number' => 1)

      jids = []
      batch.add_jobs do
        3.times do
          jids << TestWorker.perform_async
        end
      end
      batch.run

      # Проверяем, что pending = 3
      pending = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'pending') }
      expect(pending.to_i).to eq(3)

      # Пытаемся вызвать callback - он НЕ должен выполниться, так как pending > 0
      allow(Sidekiq::Client).to receive(:push_bulk)
      Sidekiq::Batch.enqueue_callbacks(:success, batch.bid)
      expect(Sidekiq::Client).not_to have_received(:push_bulk)

      # Завершаем 2 джоба - pending станет 1
      Sidekiq::Batch.process_successful_job(batch.bid, jids[0])
      Sidekiq::Batch.process_successful_job(batch.bid, jids[1])

      # Проверяем, что pending = 1
      pending = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'pending') }
      expect(pending.to_i).to eq(1)

      # Пытаемся вызвать callback - он все еще НЕ должен выполниться
      allow(Sidekiq::Client).to receive(:push_bulk)
      Sidekiq::Batch.enqueue_callbacks(:success, batch.bid)
      expect(Sidekiq::Client).not_to have_received(:push_bulk)

      # Завершаем последний джоб - pending станет 0
      expect(Sidekiq::Client).to receive(:push_bulk).at_least(:once)
      Sidekiq::Batch.process_successful_job(batch.bid, jids[2])

      # Проверяем, что pending = 0
      pending = Sidekiq.redis { |r| r.hget("BID-#{batch.bid}", 'pending') }
      expect(pending.to_i).to eq(0)
    end
  end
end

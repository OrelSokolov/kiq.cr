require 'securerandom'
require 'sidekiq'

require 'sidekiq/batch/callback'
require 'sidekiq/batch/middleware'
require 'sidekiq/batch/status'
require 'sidekiq/batch/explicit_status'
require 'sidekiq/batch/final_status_snapshot'
require 'sidekiq/batch/version'

require 'sidekiq/node'
require 'sidekiq/node_dsl'
require 'sidekiq/pipeline_dsl_extension'
require 'sidekiq/pipeline_visualizer'

# Загружаем pipeline компоненты только если ActiveRecord доступен
begin
  require 'active_record'
  require 'sidekiq/pipeline'
  require 'sidekiq/pipeline_node'
  require 'sidekiq/pipeline_callback'
  require 'sidekiq/pipeline_tracking'
rescue LoadError
  # ActiveRecord не доступен, пропускаем загрузку pipeline компонентов
  Sidekiq.logger.debug 'ActiveRecord not available, pipeline tracking disabled' if defined?(Sidekiq.logger)
end

module Sidekiq
  class Batch
    class NoBlockGivenError < StandardError; end
    class BatchAlreadyStartedError < StandardError; end

    BID_EXPIRE_TTL = 2_592_000

    # TTL для флагов обработки callbacks (24 часа)
    # Флаги хранятся отдельно от основного батча и гарантируют идемпотентность
    # даже после cleanup основного батча
    CALLBACK_FLAG_TTL = 86_400

    attr_reader :bid, :description, :callback_queue, :created_at

    def initialize(existing_bid = nil)
      @bid = existing_bid || SecureRandom.urlsafe_base64(10)
      @existing = !(!existing_bid || existing_bid.empty?) # Basically existing_bid.present?
      @initialized = false
      @created_at = Time.now.utc.to_f
      @bidkey = 'BID-' + @bid.to_s
      @queued_jids = []
      @pending_jids = []
      @jobs_block = nil
      @started = false

      @incremental_push = !Sidekiq.default_configuration[:batch_push_interval].nil?
      @batch_push_interval = Sidekiq.default_configuration[:batch_push_interval]
    end

    def description=(description)
      @description = description
      persist_bid_attr('description', description)
    end

    def callback_queue=(callback_queue)
      @callback_queue = callback_queue
      persist_bid_attr('callback_queue', callback_queue)
    end

    def callback_batch=(callback_batch)
      @callback_batch = callback_batch
      persist_bid_attr('callback_batch', callback_batch)
    end

    def on(event, callback, options = {})
      raise BatchAlreadyStartedError, 'Cannot add callbacks to a batch that has already been started' if @started
      return unless %w[success complete].include?(event.to_s)

      callback_key = "#{@bidkey}-callbacks-#{event}"
      Sidekiq.redis do |r|
        r.multi do |pipeline|
          pipeline.sadd(callback_key, [JSON.unparse({
                                                      callback: callback,
                                                      opts: options
                                                    })])
          pipeline.expire(callback_key, BID_EXPIRE_TTL)
        end
      end
    end

    def add_jobs(&block)
      raise NoBlockGivenError unless block_given?

      @jobs_block = block
      self
    end

    def run
      return [] unless @jobs_block

      @started = true

      bid_data = Thread.current[:bid_data]
      Thread.current[:bid_data] = []

      begin
        if !@existing && !@initialized
          parent_bid = Thread.current[:batch].bid if Thread.current[:batch]

          Sidekiq.redis do |r|
            r.multi do |pipeline|
              pipeline.hset(@bidkey, 'created_at', @created_at)
              pipeline.expire(@bidkey, BID_EXPIRE_TTL)
              if parent_bid
                pipeline.hset(@bidkey, 'parent_bid', parent_bid.to_s)
                pipeline.hincrby("BID-#{parent_bid}", 'children', 1)
              end
            end
          end

          @initialized = true
        end

        @queued_jids = []
        @pending_jids = []

        begin
          parent = Thread.current[:batch]
          Thread.current[:batch] = self
          Thread.current[:parent_bid] = parent_bid
          @jobs_block.call
        ensure
          Thread.current[:batch] = parent
          Thread.current[:parent_bid] = nil
        end

        # Если batch пустой (0 джобов), нужно установить pending=0 и total=0,
        # и вызвать callbacks, если они есть
        if @queued_jids.size == 0
          # Получаем parent_bid из Redis, так как он может быть установлен при инициализации
          stored_parent_bid = Sidekiq.redis { |r| r.hget(@bidkey, 'parent_bid') }
          stored_parent_bid = nil if stored_parent_bid && stored_parent_bid.empty?

          # Устанавливаем pending=0, total=0 и done=0 для пустого батча
          Sidekiq.redis do |r|
            r.multi do |pipeline|
              pipeline.hset(@bidkey, 'pending', '0')
              pipeline.hset(@bidkey, 'total', '0')
              pipeline.hset(@bidkey, 'done', '0')
              pipeline.expire(@bidkey, BID_EXPIRE_TTL)

              pipeline.expire("BID-#{stored_parent_bid}", BID_EXPIRE_TTL) if stored_parent_bid
            end
          end

          # Проверяем, есть ли callbacks для этого батча
          has_complete_callback = Sidekiq.redis { |r| r.scard("#{@bidkey}-callbacks-complete") } > 0
          has_success_callback = Sidekiq.redis { |r| r.scard("#{@bidkey}-callbacks-success") } > 0

          # Если есть callbacks, вызываем их
          # Для пустого батча без ошибок: complete и success вызываются одновременно
          if has_complete_callback || has_success_callback
            Sidekiq.logger.info "Empty batch #{@bid} has callbacks, triggering them"
            self.class.enqueue_callbacks(:complete, @bid)
            self.class.enqueue_callbacks(:success, @bid) if has_success_callback
          end

          return []
        end

        conditional_redis_increment!(true)

        Sidekiq.redis do |r|
          r.multi do |pipeline|
            pipeline.expire("BID-#{parent_bid}", BID_EXPIRE_TTL) if parent_bid

            pipeline.expire(@bidkey, BID_EXPIRE_TTL)

            pipeline.sadd(@bidkey + '-jids', @queued_jids)
            pipeline.expire(@bidkey + '-jids', BID_EXPIRE_TTL)
          end
        end

        @queued_jids
      ensure
        Thread.current[:bid_data] = bid_data
      end
    end

    def increment_job_queue(jid)
      @queued_jids << jid
      @pending_jids << jid
      conditional_redis_increment!
    end

    def conditional_redis_increment!(force = false)
      return unless should_increment? || force

      parent_bid = Thread.current[:parent_bid]
      Sidekiq.redis do |r|
        r.multi do |pipeline|
          if parent_bid
            pipeline.hincrby("BID-#{parent_bid}", 'total', @pending_jids.length)
            pipeline.expire("BID-#{parent_bid}", BID_EXPIRE_TTL)
          end

          pipeline.hincrby(@bidkey, 'pending', @pending_jids.length)
          pipeline.hincrby(@bidkey, 'total', @pending_jids.length)
          pipeline.expire(@bidkey, BID_EXPIRE_TTL)
        end
      end
      @pending_jids = []
    end

    def should_increment?
      return false unless @incremental_push
      return true if @batch_push_interval == 0 || @queued_jids.length == 1

      now = Time.now.to_f
      @last_increment ||= now
      return unless @last_increment + @batch_push_interval > now

      @last_increment = now
      true
    end

    def invalidate_all
      Sidekiq.redis do |r|
        r.multi do |pipeline|
          pipeline.set("invalidated-bid-#{@bid}", 1)
          pipeline.expire("invalidated-bid-#{@bid}", BID_EXPIRE_TTL)
        end
      end
    end

    def parent_bid
      Sidekiq.redis do |r|
        r.hget(@bidkey, 'parent_bid')
      end
    end

    def parent
      return unless parent_bid

      Sidekiq::Batch.new(parent_bid)
    end

    def valid?(batch = self)
      valid = Sidekiq.redis { |r| r.exists("invalidated-bid-#{batch.bid}") }.zero?
      batch.parent ? valid && valid?(batch.parent) : valid
    end

    private

    def persist_bid_attr(attribute, value)
      Sidekiq.redis do |r|
        r.multi do |pipeline|
          pipeline.hset(@bidkey, attribute, value)
          pipeline.expire(@bidkey, BID_EXPIRE_TTL)
        end
      end
    end

    class << self
      def process_failed_job(bid, jid)
        # Используем тот же lock что и для successful jobs для консистентности
        with_redis_lock("batch-lock-#{bid}", timeout: 5) do
          # Сначала добавляем JID в failed set и обновляем TTL
          Sidekiq.redis do |r|
            r.multi do |pipeline|
              pipeline.sadd("BID-#{bid}-failed", [jid])
              pipeline.expire("BID-#{bid}-failed", BID_EXPIRE_TTL)
            end
          end

          # Затем читаем состояние батча ПОСЛЕ добавления JID
          pending, failed, children, complete, parent_bid, total, done = Sidekiq.redis do |r|
            r.multi do |pipeline|
              pipeline.hincrby("BID-#{bid}", 'pending', 0)
              pipeline.scard("BID-#{bid}-failed")
              pipeline.hincrby("BID-#{bid}", 'children', 0)
              pipeline.scard("BID-#{bid}-complete")
              pipeline.hget("BID-#{bid}", 'parent_bid')
              pipeline.hget("BID-#{bid}", 'total')
              pipeline.hget("BID-#{bid}", 'done')
            end
          end

          # if the batch failed, and has a parent, update the parent to show one pending and failed job
          if parent_bid
            Sidekiq.redis do |r|
              r.multi do |pipeline|
                # НЕ ИНКРЕМЕНТИРУЕМ pending! Это исправляет race condition
                # Child batch считается "pending" до завершения, после чего Finalize.complete проверит состояние
                # pipeline.hincrby("BID-#{parent_bid}", 'pending', 1)
                pipeline.sadd("BID-#{parent_bid}-failed", [jid])
                pipeline.expire("BID-#{parent_bid}-failed", BID_EXPIRE_TTL)
              end
            end
          end

          # Выводим информацию о состоянии батча после неудачного джоба
          done_int = done.to_i
          max = pending.to_i + done_int
          failures_str = failed.to_i > 0 ? "FAILURES: #{failed} ".colorize(:red) : "FAILURES: #{failed} "
          Sidekiq.logger.info "PENDING: #{pending} ".colorize(:light_yellow) + "DONE: #{done_int} ".colorize(:green) + "MAX: #{max} ".colorize(:blue) + failures_str

          enqueue_callbacks(:complete, bid) if pending.to_i == failed.to_i && children == complete
        end
      end

      def process_successful_job(bid, jid)
        # Используем распределенный Redis lock для предотвращения race condition
        # Без lock'а несколько потоков могут одновременно декрементировать pending,
        # и batch может быть cleanup'нут до того, как все потоки завершат обработку
        with_redis_lock("batch-lock-#{bid}", timeout: 5) do
          failed, pending, done, children, complete, success, = Sidekiq.redis do |r|
            r.multi do |pipeline|
              pipeline.scard("BID-#{bid}-failed")
              pipeline.hincrby("BID-#{bid}", 'pending', -1)
              pipeline.hincrby("BID-#{bid}", 'done', 1)
              pipeline.hincrby("BID-#{bid}", 'children', 0)
              pipeline.scard("BID-#{bid}-complete")
              pipeline.scard("BID-#{bid}-success")
              pipeline.hget("BID-#{bid}", 'total')
              pipeline.hget("BID-#{bid}", 'parent_bid')

              pipeline.srem("BID-#{bid}-failed", [jid])
              pipeline.srem("BID-#{bid}-jids", [jid])
              pipeline.expire("BID-#{bid}", BID_EXPIRE_TTL)
            end
          end

          max = pending.to_i + done.to_i
          failures_str = failed.to_i > 0 ? "FAILURES: #{failed} ".colorize(:red) : "FAILURES: #{failed} "
          Sidekiq.logger.info "PENDING: #{pending} ".colorize(:light_yellow) + "DONE: #{done} ".colorize(:green) + "MAX: #{max} ".colorize(:blue) + failures_str

          all_success = pending.to_i.zero? && children == success
          # if complete or successfull call complete callback (the complete callback may then call successful)
          if (pending.to_i == failed.to_i && children == complete) || all_success
            enqueue_callbacks(:complete, bid)
            enqueue_callbacks(:success, bid) if all_success
          end
        end
      end

      def enqueue_callbacks(event, bid)
        Sidekiq.logger.info "ENQUEUE CALLBACKS FOR #{event} #{bid}".colorize(:light_green)

        event_name = event.to_s
        batch_key = "BID-#{bid}"
        callback_key = "#{batch_key}-callbacks-#{event_name}"

        # ОТДЕЛЬНЫЙ ключ для флага обработки - НЕ удаляется при cleanup!
        # Это гарантирует идемпотентность даже после удаления батча
        processed_flag_key = "#{batch_key}-processed-#{event_name}"

        # АТОМАРНАЯ ОПЕРАЦИЯ: используем Redis lock для атомарного чтения и пометки
        with_redis_lock("callback-lock-#{bid}-#{event_name}", timeout: 5) do
          # Проверяем флаг обработки в ОТДЕЛЬНОМ ключе (не удаляется при cleanup)
          already_processed = Sidekiq.redis { |r| r.get(processed_flag_key) }

          if already_processed == 'true'
            Sidekiq.logger.debug "Callback #{event_name} for batch #{bid} already processed, skipping"
            return
          end

          # КРИТИЧЕСКАЯ ПРОВЕРКА: callback не должен выполняться, пока pending > 0
          # Это гарантирует синхронизацию - callback выполнится только после завершения всех джобов
          pending, children, complete, success, failed = Sidekiq.redis do |r|
            r.multi do |pipeline|
              pipeline.hget(batch_key, 'pending')
              pipeline.hget(batch_key, 'children')
              pipeline.scard("#{batch_key}-complete")
              pipeline.scard("#{batch_key}-success")
              pipeline.scard("#{batch_key}-failed")
            end
          end

          pending = pending.to_i
          children = children.to_i
          complete = complete.to_i
          success = success.to_i
          failed = failed.to_i

          # Для complete callback: pending должен быть равен failed (все джобы завершены или провалены)
          # И все дочерние батчи должны быть завершены (children == complete)
          if (event_name == 'complete') && (pending != failed || children != complete)
            Sidekiq.logger.debug "Callback #{event_name} for batch #{bid} skipped: pending=#{pending}, failed=#{failed}, children=#{children}, complete=#{complete}"
            return
          end

          # Для success callback: pending должен быть 0 И все дочерние батчи успешны (children == success)
          if (event_name == 'success') && (pending != 0 || children != success)
            Sidekiq.logger.debug "Callback #{event_name} for batch #{bid} skipped: pending=#{pending}, children=#{children}, success=#{success}"
            return
          end

          callbacks, queue, parent_bid, callback_batch = Sidekiq.redis do |r|
            r.multi do |pipeline|
              # Читаем callbacks (даже если батч уже удален, callbacks могут еще существовать)
              pipeline.smembers(callback_key)
              # Читаем метаданные батча
              pipeline.hget(batch_key, 'callback_queue')
              pipeline.hget(batch_key, 'parent_bid')
              pipeline.hget(batch_key, 'callback_batch')
            end
          end

          # СРАЗУ помечаем событие как обработанное в ОТДЕЛЬНОМ ключе с TTL
          # Это предотвращает race condition и гарантирует идемпотентность
          # Даже если батч будет удален, флаг останется на 24 часа
          Sidekiq.redis do |r|
            r.set(processed_flag_key, 'true', ex: CALLBACK_FLAG_TTL)
          end

          # Если callbacks пусты - это нормально, если они не были зарегистрированы
          # Мы всё равно вызываем Finalize для корректного cleanup
          if callbacks.nil? || callbacks.empty?
            Sidekiq.logger.debug "No callbacks registered for #{event_name} on batch #{bid}"
            # Всё равно вызываем Finalize для корректного cleanup (если это не callback batch)
            unless callback_batch
              finalizer = Sidekiq::Batch::Callback::Finalize.new
              status = Status.new bid
              finalizer.dispatch(status, { 'bid' => bid, 'event' => event_name })
            end
            return
          end

          queue ||= 'default'
          parent_bid = !parent_bid || parent_bid.empty? ? nil : parent_bid # Basically parent_bid.blank?
          callback_args = callbacks.reduce([]) do |memo, jcb|
            cb = Sidekiq.load_json(jcb)
            serialized_status = Sidekiq::Batch::FinalStatusSnapshot.new(bid).serialized
            memo << [cb['callback'], event_name, cb['opts'], bid, parent_bid, serialized_status]
          end

          opts = { 'bid' => bid, 'event' => event_name }

          # Удаляем callbacks после их чтения
          # Это безопасно, так как callbacks уже прочитаны и поставлены в очередь Sidekiq
          Sidekiq.redis { |r| r.del(callback_key) }

          # Run callback batch finalize synchronously
          if callback_batch
            # Extract opts from cb_args or use current
            # Pass in stored event as callback finalize is processed on complete event
            cb_opts = callback_args.first&.at(2) || opts

            Sidekiq.logger.debug { "Run callback batch bid: #{bid} event: #{event_name}" }
            # Finalize now
            finalizer = Sidekiq::Batch::Callback::Finalize.new
            status = Status.new bid
            finalizer.dispatch(status, cb_opts)

            return
          end

          Sidekiq.logger.debug { "Enqueue callback bid: #{bid} event: #{event_name} args: #{callback_args.inspect}" }

          if callback_args.empty?
            # Finalize now
            finalizer = Sidekiq::Batch::Callback::Finalize.new
            status = Status.new bid
            finalizer.dispatch(status, opts)
          else
            # Otherwise finalize in sub batch complete callback
            cb_batch = new
            cb_batch.callback_batch = 'true'
            Sidekiq.logger.debug { "Adding callback batch: #{cb_batch.bid} for batch: #{bid}" }
            cb_batch.on(:complete, 'Sidekiq::Batch::Callback::Finalize#dispatch', opts)
            cb_batch.add_jobs do
              push_callbacks callback_args, queue
            end
            cb_batch.run
          end
        end
      end

      def with_redis_lock(lock_key, timeout: 30, max_wait: 60)
        # Генерируем уникальный токен для этого lock'а
        lock_token = SecureRandom.uuid
        locked = false
        start_time = Time.now
        result = nil

        begin
          # Пытаемся получить lock с повторными попытками
          loop do
            locked = Sidekiq.redis do |r|
              # SET NX EX - устанавливает ключ только если его нет, с автоматическим expire
              r.set(lock_key, lock_token, nx: true, ex: timeout)
            end

            break if locked

            # Проверяем timeout
            if Time.now - start_time > max_wait
              raise "Failed to acquire Redis lock '#{lock_key}' after #{max_wait} seconds"
            end

            # Очень короткая задержка с jitter для минимизации contention
            sleep(0.0001 + rand * 0.0001)
          end

          # Выполняем блок кода под lock'ом
          result = yield
        ensure
          # Освобождаем lock только если мы его держим (проверяем токен)
          if locked
            Sidekiq.redis do |r|
              # Проверяем что lock всё ещё наш, потом удаляем
              current_token = r.get(lock_key)
              r.del(lock_key) if current_token == lock_token
            end
          end
          result 
        end
      end

      private

      def push_callbacks(args, queue)
        return if args.empty?

        Sidekiq::Client.push_bulk(
          'class' => Sidekiq::Batch::Callback::Worker,
          'args' => args,
          'queue' => queue
        )
      end
    end
  end
end

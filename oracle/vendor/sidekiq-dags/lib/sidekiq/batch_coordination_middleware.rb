# frozen_string_literal: true

module Sidekiq
  # Client Middleware: отслеживает регистрацию джоб в batch
  # Увеличивает счётчик total_registered только для джоб, которые реально попали в batch
  class BatchCoordinationClientMiddleware
    def call(worker_class, job, queue, redis_pool)
      # Если джоба создается внутри batch context
      if batch = Thread.current[:batch]
        batch_id = batch.bid
        worker_name = worker_class.is_a?(String) ? worker_class : worker_class.name
        
        # Не считаем DummyJob как "реальную работу"
        unless dummy_job?(worker_name)
          # Выполняем yield ПЕРВЫМ, чтобы джоба реально попала в batch
          # (через Sidekiq::Batch::Middleware::ClientMiddleware, который вызывается после нас)
          result = yield
          
          # Проверяем, что джоба реально попала в batch (имеет bid)
          # job['bid'] устанавливается в Sidekiq::Batch::Middleware::ClientMiddleware
          # Если джоба не попала в batch, не увеличиваем счетчик
          if job['bid'] == batch_id
            meta_key = "batch-#{batch_id}-coordination"
            
            Sidekiq.redis do |conn|
              # Увеличиваем счетчик зарегистрированных джоб
              # executed_jobs будет увеличиваться при реальном выполнении
              conn.hincrby(meta_key, "total_registered", 1)
              conn.hset(meta_key, "last_registered_at", Time.now.to_f)
              conn.expire(meta_key, 3600) # 1 час TTL
            end
            
            Sidekiq.logger.debug "[BATCH_COORD] Registered #{worker_name} in batch #{batch_id}"
          else
            Sidekiq.logger.debug "[BATCH_COORD] Job #{worker_name} not added to batch #{batch_id} (bid mismatch: #{job['bid']} != #{batch_id})"
          end
          
          result
        else
          yield
        end
      else
        yield
      end
    end
    
    private
    
    def dummy_job?(name)
      name.nil? || 
        name == 'DummyJob' || 
        name == 'Sidekiq::DummyJob' ||
        name.end_with?('::DummyJob')
    end
  end
  
  # Server Middleware: координирует выполнение джоб в batch
  # 1. Для реальных джоб: увеличивает executed_jobs после завершения
  # 2. Для DummyJob: ждёт пока все реальные джобы завершатся
  class BatchCoordinationServerMiddleware
    MAX_WAIT = 30           # максимум 30 секунд ждем завершения всех джоб
    STABILITY_WINDOW = 0.5  # 500ms без новых регистраций = стабильность
    POLL_INTERVAL = 0.1     # проверяем каждые 100ms
    
    def call(worker, job, queue)
      batch_id = job['bid']
      worker_name = worker.class.name
      is_dummy = dummy_job?(worker_name)
      meta_key = batch_id ? "batch-#{batch_id}-coordination" : nil
      
      if batch_id && is_dummy
        # DummyJob должен подождать пока все реальные джобы завершатся
        wait_for_real_jobs_completion(batch_id, meta_key)
      end
      
      begin
        yield
      ensure
        # После завершения реальной джобы - увеличиваем счетчик выполненных
        if batch_id && !is_dummy && meta_key
          executed = Sidekiq.redis do |conn|
            conn.hincrby(meta_key, "executed_jobs", 1)
          end
          Sidekiq.logger.debug "[BATCH_COORD] Job #{worker_name} completed in batch #{batch_id}, #{executed} jobs executed"
        end
      end
    end
    
    private
    
    def dummy_job?(name)
      name.nil? || 
        name == 'DummyJob' || 
        name == 'Sidekiq::DummyJob' ||
        name.end_with?('::DummyJob')
    end
    
    def wait_for_real_jobs_completion(batch_id, meta_key)
      start_time = Time.now
      last_registered_at = nil
      
      loop do
        total_registered, executed_jobs, last_reg = Sidekiq.redis do |conn|
          [
            conn.hget(meta_key, "total_registered").to_i,
            conn.hget(meta_key, "executed_jobs").to_i,
            conn.hget(meta_key, "last_registered_at")
          ]
        end
        
        last_registered_at = last_reg.to_f if last_reg
        
        # Если никаких реальных джоб не было зарегистрировано - продолжаем сразу
        if total_registered == 0
          Sidekiq.logger.debug "[BATCH_COORD] No real jobs were registered in batch #{batch_id}, proceeding"
          return
        end
        
        # Если все зарегистрированные джобы выполнены - продолжаем
        if executed_jobs >= total_registered
          Sidekiq.logger.info "[BATCH_COORD] All #{total_registered} real jobs executed in batch #{batch_id}, DummyJob proceeding"
          return
        end
        
        # Проверяем стабильность: если прошло STABILITY_WINDOW с последней регистрации
        # и все зарегистрированные джобы выполнены, можно продолжать
        elapsed = Time.now - start_time
        if last_registered_at && elapsed > STABILITY_WINDOW
          time_since_last_reg = Time.now.to_f - last_registered_at
          if time_since_last_reg >= STABILITY_WINDOW && executed_jobs >= total_registered
            Sidekiq.logger.info "[BATCH_COORD] Batch #{batch_id} stable (#{time_since_last_reg.round(2)}s since last registration), all #{executed_jobs}/#{total_registered} jobs executed, proceeding"
            return
          end
        end
        
        # Таймаут безопасности
        if elapsed > MAX_WAIT
          pending = total_registered - executed_jobs
          Sidekiq.logger.warn "[BATCH_COORD] Timeout waiting for #{pending} jobs in batch #{batch_id} (waited #{elapsed.round(1)}s, executed: #{executed_jobs}/#{total_registered}), proceeding anyway"
          return
        end
        
        Sidekiq.logger.debug "[BATCH_COORD] Waiting for batch #{batch_id}: executed #{executed_jobs}/#{total_registered} (#{elapsed.round(1)}s elapsed)"
        sleep POLL_INTERVAL
      end
    end
  end
end


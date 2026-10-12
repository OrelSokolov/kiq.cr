# Batch Coordination Middleware

## Проблема

При использовании `Sidekiq::Node` с rate limiting (например, через `sidekiq-uniform`) возникает race condition:

```ruby
node :CalendarNode, BaseNode do
  execute do
    # CalendarJob имеет rate limiting (5 RPS)
    CalendarJob.perform_async(start_date, end_date)
  end
end
```

**Последовательность событий:**

1. Node создает batch с `pending = 1` (DummyJob)
2. DummyJob выполняется мгновенно (нет rate limiting)
3. DummyJob декрементирует `pending` → 0
4. Batch callbacks срабатывают
5. **Но CalendarJob еще не успела зарегистрироваться** из-за rate limiter задержки!
6. Node считается завершенной, переходит к следующей
7. CalendarJob регистрируется слишком поздно → **race condition**

## Решение

Два middleware работают вместе для координации:

### 1. Client Middleware (отслеживание регистраций)

```ruby
class BatchCoordinationClientMiddleware
  def call(worker_class, job, queue, redis_pool)
    if batch = Thread.current[:batch]
      # Для каждой "реальной" джобы (не DummyJob)
      unless worker_class == 'DummyJob'
        # Увеличиваем счетчик в Redis
        # Сохраняем timestamp последней регистрации
      end
    end
    yield
  end
end
```

**Записывает в Redis:**
- `batch-{bid}-meta:real_jobs` - количество реальных джоб
- `batch-{bid}-meta:last_job_registered_at` - timestamp последней регистрации

### 2. Server Middleware (координация DummyJob)

```ruby
class BatchCoordinationServerMiddleware
  def call(worker, job, queue)
    if worker.is_a?(DummyJob) && job['bid']
      wait_for_batch_stability(batch_id)
    end
    yield
  end
  
  def wait_for_batch_stability(batch_id)
    # Ждет пока:
    # 1. Нет реальных джоб (можно завершаться сразу)
    # 2. ИЛИ прошло STABILITY_WINDOW (200ms) с последней регистрации
    # 3. ИЛИ истек MAX_WAIT (5s) - таймаут безопасности
  end
end
```

## Как это работает

### Пример с rate limiting

```ruby
# В Node#perform
@batch.add_jobs do
  DummyJob.perform_async(desc)              # bid=ABC
  CalendarJob.perform_async(start, end)    # bid=ABC
end
```

**Timeline:**

```
t=0ms:   add_jobs начинается
         └─ Thread.current[:batch] = ABC
         
t=1ms:   DummyJob.perform_async
         └─ Client middleware: worker == DummyJob → skip
         └─ DummyJob отправлена в очередь
         
t=2ms:   CalendarJob.perform_async
         └─ Client middleware: worker != DummyJob
         └─ Redis: batch-ABC-meta:real_jobs = 1
         └─ Redis: batch-ABC-meta:last_job_registered_at = 1234567890.002
         └─ Rate limiter задерживает отправку на 150ms
         
t=150ms: CalendarJob отправлена в очередь
         
t=10ms:  DummyJob начинает выполняться
         └─ Server middleware проверяет: real_jobs = 1
         └─ Ждет стабильности...
         
t=200ms: Прошло 50ms с последней регистрации (200-150)
         └─ Стабильность достигнута (STABILITY_WINDOW = 200ms)
         └─ DummyJob продолжает выполнение
         └─ DummyJob завершается
         └─ pending декрементируется
         └─ Callbacks срабатывают КОРРЕКТНО
```

## Параметры настройки

```ruby
class BatchCoordinationServerMiddleware
  STABILITY_WINDOW = 0.2  # 200ms без новых регистраций
  MAX_WAIT = 5            # максимальное ожидание 5 секунд
  POLL_INTERVAL = 0.05    # проверка каждые 50ms
end
```

### Настройка под ваш rate limiter

Если ваш rate limiter может задерживать джобы более чем на 200ms:

```ruby
# В вашем приложении (config/initializers/sidekiq_dags.rb)
Sidekiq::BatchCoordinationServerMiddleware::STABILITY_WINDOW = 0.5  # 500ms
```

## Overhead

- **Client middleware**: ~0.1ms на джобу (один Redis hset)
- **Server middleware**: 
  - 0ms если нет реальных джоб
  - ~200ms ожидание для стабильности
  - Redis запрос каждые 50ms во время ожидания

## Тестирование

Запустите mosff pipeline который ранее зависал:

```ruby
Mosff::RootNode.perform_async
```

Проверьте логи - должны увидеть:

```
[BATCH_COORDINATION] Registered Mosff::CalendarJob in batch XYZ
[BATCH_COORDINATION] Batch XYZ stable (1 real jobs registered), proceeding
```

## Отключение (если нужно)

Middleware регистрируются автоматически. Для отключения:

```ruby
# config/initializers/sidekiq.rb
Sidekiq.configure_server do |config|
  config.server_middleware do |chain|
    chain.remove Sidekiq::BatchCoordinationServerMiddleware
  end
end

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.remove Sidekiq::BatchCoordinationClientMiddleware
  end
end
```


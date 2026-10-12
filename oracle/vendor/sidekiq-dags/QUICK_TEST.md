# Быстрое тестирование Batch Coordination

## RSpec тесты

### Все coordination тесты
```bash
cd ~/sidekiq-dags
bundle exec rake test:coordination
```

### Или через rspec напрямую
```bash
bundle exec rspec spec/integration/batch_coordination_spec.rb --format documentation
```

### Конкретный тест
```bash
bundle exec rspec spec/integration/batch_coordination_spec.rb:45
```

## Ручной тест (интерактивный)

### Базовый тест
```bash
bundle exec rake test:coordination_manual
```

### С кастомной задержкой
```bash
DELAY_MS=500 bundle exec rake test:coordination_manual
```

### С другим Redis
```bash
REDIS_URL=redis://localhost:6379/10 bundle exec rake test:coordination_manual
```

## Что проверять

### ✅ Успешный тест должен показать:
```
⏱️  [13:39:28.280] Rate limiter: delaying by 300ms
[BATCH_COORDINATION] Registered RateLimitedJob in batch ABC123
[BATCH_COORDINATION] Batch ABC123 stable (1 real jobs registered), proceeding
✅ [13:39:28.590] RateLimitedJob executed: 42

✅ Test completed in 0.503s
Expected: ~0.5s (300ms delay + 200ms stability)
Actual: 0.503s

🎉 SUCCESS: Coordination working correctly!
```

### ❌ Без coordination было бы:
```
✅ [13:39:28.285] RateLimitedJob executed: 42  ← слишком рано!
Duration: 0.005s  ← race condition!
```

## Тестирование в вашем проекте

После изменений в геме:

```bash
cd ~/ds/parsing

# 1. Перезапустите Sidekiq
pkill -f sidekiq && bundle exec sidekiq -q default,5 ...

# 2. Rails console
bundle exec rails console
```

```ruby
# 3. Очистите mosff pipeline
SidekiqPipeline.where(name: 'mosff').update_all(status: 'idle')
SidekiqPipelineNode.where(pipeline_name: 'mosff').update_all(status: 'pending')

# 4. Запустите
Mosff::RootNode.perform_async

# 5. Проверьте логи - должны быть:
# [BATCH_COORDINATION] Registered Mosff::CalendarJob
# [BATCH_COORDINATION] Batch stable, proceeding
# ✅ Node mosff::CalendarNode completed
```

## Параметры

Можно настроить в коде middleware:

```ruby
# lib/sidekiq/batch_coordination_middleware.rb
class BatchCoordinationServerMiddleware
  STABILITY_WINDOW = 0.2  # Увеличьте если rate limiter > 200ms
  MAX_WAIT = 5            # Таймаут безопасности
  POLL_INTERVAL = 0.05    # Частота проверки
end
```


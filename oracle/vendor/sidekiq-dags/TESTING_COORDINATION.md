# Тестирование Batch Coordination

## Что было исправлено

**Проблема:** Пайплайн Mosff зависал на CalendarNode из-за race condition:
- DummyJob выполнялась мгновенно
- CalendarJob задерживалась rate limiter'ом (5 RPS)
- Batch callbacks срабатывали ДО регистрации CalendarJob
- Node считалась завершенной, но реальная работа не началась

**Решение:** Middleware координация - DummyJob ждет пока все джобы зарегистрируются.

## Как протестировать в вашем проекте

### 1. Перезапустите Sidekiq

Gem изменен, нужно перезагрузить:

```bash
cd ~/ds/parsing

# Если Sidekiq запущен через systemd
sudo systemctl restart sidekiq

# Или если через screen/tmux - найдите процесс и перезапустите
pkill -f sidekiq
bundle exec sidekiq -q default,5 -q mosff,1 -q rustat,1 ...
```

### 2. Очистите зависшие pipeline

```bash
cd ~/ds/parsing
bundle exec rails console

# Очистите зависший mosff pipeline
SidekiqPipeline.where(name: 'mosff').update_all(status: 'idle')
SidekiqPipelineNode.where(pipeline_name: 'mosff').update_all(status: 'pending')
```

### 3. Запустите Mosff pipeline

```ruby
# В rails console
Mosff::RootNode.perform_async

# Или через UI
# Откройте http://localhost:3000/pipeline_graph
# Нажмите "Start Pipeline" для mosff
```

### 4. Смотрите логи

В логах Sidekiq должны появиться новые сообщения:

```
[BATCH_COORDINATION] Registered Mosff::CalendarJob in batch ABC123
[BATCH_COORDINATION] Batch ABC123 stable (1 real jobs registered), proceeding
✅ Node mosff::CalendarNode completed
➡️ Triggering next node: Mosff::MatchesNode
```

**Что проверить:**
- ✅ Node не зависает в статусе "running"
- ✅ CalendarJob выполняется
- ✅ Pipeline переходит к следующей ноде (MatchesNode)
- ✅ Нет warning'ов о race condition

### 5. Проверьте timing

Посмотрите разницу во времени между событиями:

```
INFO  13:39:28.280Z - DummyJob START
INFO  13:39:28.296Z - CalendarJob START (задержка 16ms от DummyJob)
[BATCH_COORDINATION] Batch stable - DummyJob ждала регистрации!
INFO  13:39:28.327Z - CalendarJob FINISH
INFO  13:39:28.328Z - Callbacks triggered
```

**Раньше было:**
```
13:39:28.280Z - DummyJob START
13:39:28.285Z - DummyJob FINISH (5ms)
13:39:28.286Z - CALLBACKS (ошибка - CalendarJob еще не зарегистрирована!)
13:39:28.296Z - CalendarJob START (слишком поздно...)
```

## Дополнительные тесты

### Тест с другими pipelines

Проверьте что другие pipelines не сломались:

```ruby
# Rustat
Rustat::RootNode.perform_async

# Transfermarkt  
Transfermarkt::RootNode.perform_async

# Bsight
Bsight::RootNode.perform_async
```

Все должны работать нормально (middleware прозрачный для них).

### Проверка Redis metadata

Во время выполнения pipeline:

```ruby
# Найдите batch_id из логов (bid=ABC123)
batch_id = "ABC123"

# Проверьте metadata
Sidekiq.redis do |conn|
  real_jobs = conn.hget("batch-#{batch_id}-meta", "real_jobs")
  timestamp = conn.hget("batch-#{batch_id}-meta", "last_job_registered_at")
  
  puts "Real jobs: #{real_jobs}"
  puts "Last registered at: #{Time.at(timestamp.to_f)}"
end
```

## Если что-то не работает

### Debug режим

Включите debug логирование в Sidekiq:

```ruby
# config/initializers/sidekiq.rb
Sidekiq.configure_server do |config|
  Sidekiq.logger.level = Logger::DEBUG
end
```

Перезапустите Sidekiq и смотрите детальные логи middleware.

### Проверьте middleware зарегистрированы

```ruby
# В rails console
Sidekiq.server_middleware.entries.map(&:klass)
# Должен быть Sidekiq::BatchCoordinationServerMiddleware

Sidekiq.client_middleware.entries.map(&:klass)  
# Должен быть Sidekiq::BatchCoordinationClientMiddleware
```

### Откатиться назад

Если middleware вызывает проблемы:

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

## Что дальше

После успешного тестирования:
1. Мониторьте performance (middleware добавляет ~200ms задержки для стабильности)
2. Если нужно настроить параметры - см. `docs/batch_coordination.md`
3. Сообщите о результатах тестирования


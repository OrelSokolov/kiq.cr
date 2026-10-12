# Changelog

## [1.0.1] - 2026-04-07

### Fixed
- **Исправлена проблема с Redis-соединениями в многопоточной среде**
  - `PipelineStatus.redis` теперь принимает блок вместо возврата соединения
  - Это предотвращает ошибку "stream closed in another thread" при высокой нагрузке
  - Все методы `PipelineStatus` обновлены для использования блоков

## [Unreleased]

### Added
- **Batch Coordination Middleware** - решает проблему race condition при использовании rate limiting в нодах
  - `BatchCoordinationClientMiddleware` - отслеживает регистрацию реальных джоб в batch
  - `BatchCoordinationServerMiddleware` - координирует выполнение DummyJob, ожидая стабильности batch
  - Автоматическая регистрация middleware при загрузке гема
  - Поддержка любых rate limiters (sidekiq-uniform, sidekiq-throttled, etc.)
  
### Fixed
- Race condition когда DummyJob завершается до регистрации джоб с rate limiting
- Преждевременное срабатывание batch callbacks в нодах с медленной регистрацией джоб

### Technical Details
- DummyJob теперь ожидает "стабильности" batch перед завершением
- Стабильность = 200ms без новых регистраций джоб
- Максимальное ожидание = 5 секунд (таймаут безопасности)
- Минимальный overhead: ~0.1ms на джобу для client middleware
- См. `docs/batch_coordination.md` для деталей

## [Previous versions]
...

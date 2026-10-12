# kiq.cr — порт Sidekiq (Kiq)

> ⚠️ EXPERIMENTAL FRAMEWORK, DO NOT USE IN PRODUCTION YET.

Неймспейс: `Kiq` — 1:1 к Sidekiq 8.1.7 (`Sidekiq→Kiq`,
`sidekiq_options→kiq_options`). Wire-формат байт-совместим с
настоящим Sidekiq: одни ключи Redis (`queue:<name>` LIST, `queues` SET,
`schedule`/`retry`/`dead` ZSET, `stat:processed[:date]`), один
пейлоад (jid-хекс-24, created_at/enqueued_at в epoch-ms — как sidekiq
8.1.x). Джобу может поставить sidekiq-клиент на Ruby — выполнит Kiq,
и наоборот (проверено фикс-пейлоадом в spec/unit_spec.cr).

## API 1:1

### DSL
```crystal
class HardJob
  include Kiq::Job            # include Kiq::Worker — алиас
  kiq_options queue: "critical", retry: 5, dead: true, retry_queue: "low"

  def perform(*args)
  end
end
```
- `HardJob.perform_async(...)`, `perform_in(sec, ...)`,
  `perform_at(time, ...)`, `perform_bulk([[..], [..]])`,
  `perform_inline(...)`
- `HardJob.set(queue:, retry:, wait:, wait_until:, sync:).perform_async(...)`
  (`Kiq::Job::Setter`)
- `retry:` true/число (25 по умолчанию), `dead: false` — discard,
  `retry_queue:` — ретраи в другую очередь

### Client
- `Kiq::Client.push(item) / .push_bulk(items)` — нормализация 1:1
  (class/queue/args/jid/created_at), LPUSH+`queues` SADD; `at:` →
  ZSET `schedule` (score=epoch-float, пейлоад без `at`/`enqueued_at`)
- `Kiq.redis { |c| ... }`, `Kiq.configure_server/client/configure`
  (блоки `configure_server` выполняет Launcher — как CLI оригинала)
- `Kiq::Config`: redis url, concurrency=5, timeout=25,
  max_retries=25, dead_max_jobs=10_000, dead_timeout=180d,
  poll_interval_average, очереди с весами (strict/random/weighted)

### Server
- `Kiq::Launcher` (processors + `Kiq::Scheduled::Poller` + heartbeat
  flush): взвешенный/strict BRPOP (timeout 2s), graceful stop,
  счётчики `stat:processed[:date]`/`stat:failed[:date]` (TTL 5 лет,
  инкремент при остановке/раз в 10 c)
- Retry 1:1: delay = `count⁴ + 15 + rand(10·(count+1))`, ZSET `retry`,
  `failed_at`/`retried_at`/`retry_count`/`error_class`/`error_message`;
  исчерпание → morgue (ZSET `dead`, zremrangebyrank/score как в
  оригинале), death_handlers; `retry: false/0` → discard/сразу dead
- Scheduled poller: ZRANGEBYSCORE + win-by-ZREM (у sidekiq — Lua-скрипт;
  гарантия exactly-once та же, без кэша скриптов)

### API
- `Kiq::Queue` (size/latency/clear/all), `Kiq::Stats`
  (processed/failed/enqueued/queues/scheduled_size/...),
  `Kiq::ScheduledSet` / `RetrySet` / `DeadSet` (size/clear/entries)

### Middleware
- `Kiq::Middleware::Chain` (add/prepend/remove/exists?/clear) +
  client (`Kiq::Middleware::ClientMiddleware#call(job_class, msg,
  queue)`) / server (`ServerMiddleware#call(job, msg, queue)`) цепочки;
  клиентский middleware может остановить push (nil)

### Testing
- `Kiq.testing!("fake"/"inline"/"disable")` + `Kiq::Testing.fake!/
  inline!/disable!` (блочная форма), `HardJob.jobs/clear/drain/
  perform_one`, `Kiq::Queues[...]`/`clear_all`

### ActiveJob-адаптер
`require "kiq_cr/active_job_adapter"` → `ActiveJob.adapter =
ActiveJob::KiqAdapter.new` (JobWrapper с `wrapped`, 1:1 к
SidekiqAdapter Rails; `scheduled_at` → `at`)

## Оракул (правило AGENTS.md)

`oracle/cases.rb` — 29 кейсов, снятых с настоящего sidekiq 8.1.7
(Ruby 3.4.5, локальный Redis DB 15, TZ=UTC): структура пейлоадов,
кейс-set-опций, strict-порядок исполнения, retry-математика в
детерминированных границах формулы, исчерпание ретраев/morgue/
discard, promotion через поллер, Queue/Stats/сеты, testing-режимы,
middleware. Нормализация jid/timestamps → повторный прогон
DETERMINISTIC. Зеркало `spec/oracle_spec.cr` проходит 29/29 без правок
кейсов. Source-эталон: `oracle/vendor/sidekiq-8.1.7/`.

### Web UI (Sidekiq::Web 8.1.7 → Kiq::Web)

`oracle/web_cases.rb` — 94 маркера с настоящего Sidekiq::Web 8.1.7
(Rack::MockRequest против реального sidekiq: статусы, редиректы,
структурные маркеры HTML, состояние Redis после мутаций) →
`oracle/expected_web.yml`; зеркало `spec/web_oracle_spec.cr` — 94/94
(поднимает `HTTP::Server` с `Kiq::Web` на 127.0.0.1:0; требует Redis,
иначе pending). Маркеры ловят поведение 1:1, включая особенности
оригинала: фильтр `?substr=` ищет подстроку по пейлоаду, CSP
`script-src 'self' 'nonce-…'`, CSRF-lite (POST без
`Sec-Fetch-Site: same-origin` → 403 «Forbidden»), 404 «Not Found»
текстом, HEAD-хартбит `/` (llen queue:default), Location-редиректы,
`data-nwp`-спаны чисел, Ruby-inspect аргументов с HTML-escaping,
пагинация LIST-очередей (LRANGE, ?page/?count, ?direction=asc),
score-jid ключи, сигналы TSTP/TERM в `<identity>-signals`.

Реализация: `src/web.cr` (роутер 36 маршрутов 1:1, ассеты зашиты
`{{ read_file }}`, локали en/ru line-parser'ом как в оригинале,
CSRF/Sec-Fetch-Site, percent-decode пути), `src/web/action.cr`
(`Ctx` + `Action`: параметры Rack-стиля `key[]`→массив, HMAC-сессия
cookie `kiq.session`, Accept-Language matching, Paginator),
`src/web/views/views.cr` + `*.ecr` (перевод всех ERB-вьюх),
`src/web/metrics.cr` (Metrics::Tracking — counts-only).

Часть кейсов оракула содержит Ruby-баг «truthy `has(...)`» — хелпер
возвращает строки "yes"/"no", а кейс ветвится на них как на булевы,
поэтому всегда срабатывает первая ветка (16 кейсов); в зеркале они
зафиксированы константами (помечены TRUTHY). Для брендинга
(`Sidekiq` vs `Kiq`) кейсы `web_dash_title`/`web_dash_footer_product`
и Location-редиректы нормализуют имя продукта/base_url — обе версии
проходят одни и те же кейсы.

### Batch + DAG pipelines (sidekiq-dags → Kiq)

`oracle/dags_cases.rb` → `oracle/expected_dags.yml` (59 кейсов,
`oracle/vendor/sidekiq-dags/`); зеркало `spec/dags_oracle_spec.cr` —
60/60. `src/batch.cr` (Batch/Status/ExplicitStatus/FinalStatusSnapshot
— JSON вместо Marshal, Callback::Registry, middleware,
with_redis_lock), `src/dags.cr` (Pipeline/PipelineNode/PipelineStatus),
`src/node.cr` (Kiq::Node + DSL-макросы pipeline/node, desc/execute/
next_node). Подключается в конце `src/kiq_cr.cr` (после тела module
Kiq — иначе недоступен `Kiq::JOBS`).

## Отклонения от sidekiq (задокументировано)

1. `configure_server` standalone не исполняет блок немедленно — как в
   оригинале без CLI (блоки запускает Launcher; в оракуле конфиг
   мутируется напрямую — тот же путь, что проходит CLI)
2. Класс джобы — compile-time registry (`macro included` + arity-
   dispatch из сигнатуры `perform`, техника active_job.cr); runtime
   const-get нет. Unknown class → error_handlers без ретрая
3. `raise "string"` → error_class `RuntimeError` (в Crystal это
   plain Exception — маппинг для wire-совместимости)
4. Middleware хранит инстансы, не классы (инстанцирование per-chain,
   не per-job)
5. Poller: ZRANGEBYSCORE+ZREM вместо Lua ZPOPBYSCORE
6. Web UI: `Kiq::Web` — HTTP::Handler в корне (`HTTP::Server.new([Kiq::Web.new])`),
   mount-префикса нет. Metrics — counts-only (`Metrics::Tracking`
   пишет `stat:<class>:<date>` `p/f<минута>`, не регистрируется в
   дефолтной цепочке; гистограммы времени исполнения не пишутся,
   таблицы метрик пустые); profiles — заглушки (`/profiles/:key` →
   redirect на /profiles, ProfileSet не портирован); WorkSet пуст
   (per-job work-записи не ведутся — /busy показывает процессы без
   работающих джоб). Сессия — HMAC-SHA256 cookie `kiq.session`
   (эквивалент Rack::Session::Cookie, flash не поддержан).
   `enqueued_at`/`created_at` Kiq пишет в epoch-ms (sidekiq — float-сек);
   `Queue#latency` понимает оба формата. Брендинг: NAME «Kiq»
   (титул `[DEVELOPMENT] Kiq`, футер `Kiq v8.1.7`) — кейсы оракула
   нормализуют `(?:Sidekiq|Kiq) v…` → `<product>`
7. Batch/DAG: снапшоты статусов — JSON-строки вместо Marshal;
   callback-воркеры — compile-time registry вместо const-get
7. Нет в P0: capsules, Redis
   namespace, batch/Pro-API, `sidekiq_retry_in`/
   `sidekiq_retries_exhausted` блоки (P1), backtrace-сжатие,
   I18n-локаль джоб
8. Аргументы — JSON-нативные (`Kiq::Arg`); Symbol → String
9. Тестовый режим процессно-глобален (у Ruby — thread-local оверрайд)

## Запуск воркеров

**Бинарник `kiq`** (аналог команды `sidekiq`, `src/kiq_cli.cr`,
сборка `rake build:kiq` или `shards build kiq` → `bin/kiq`):

```sh
./bin/kiq -c 10 -q critical -q default,2 -t 25 -v
```

- флаги sidekiq-стиля: `-c` concurrency, `-q NAME[,WEIGHT]` (повторяемый,
  вес по умолчанию 1), `-t` timeout, `-v` verbose, `-V` версия;
  Redis — из `REDIS_URL`
- сигналы: `TERM`/`INT` — graceful stop (до `-t` секунд на джобы,
  flush статистики, exit 0), `TSTP` — quiet (новых джоб не берём)
- boot-ping: недоступный Redis → сообщение и exit 1
- воркер-цикл переживает потерю Redis (error_handler + retry через 1 c)
- **джобы запекаются при сборке** (compile-time require, runtime `-r`
  невозможен в Crystal): `KIQ_REQUIRE=../app/jobs/** shards build kiq`
  (путь относительно `src/` шарда) — либо свой entry-point в приложении:

```crystal
# worker.cr
require "./app/jobs/**"
require "kiq_cr/src/kiq_cli"   # парсит флаги и запускает CLI
# crystal build --release worker.cr -o bin/kiq-worker
```

Generic-бинарь без джоб предупреждает при старте. CLI-спек
(`spec/cli_spec.cr`, pending пока `bin/kiq` не собран) гоняет бинарник
end-to-end: -V, boot-ping, unknown-class discard, TERM-graceful.

## Статус

- P0 готов: oracle 29/29 + юниты 7/7 (interop-фикстура настоящего
  sidekiq → исполнение Kiq; обратная совместимость пейлоада; weighted
  queues; graceful stop; ActiveJob-адаптер; Testing) + CLI 3/3
- Web UI: oracle 94/94 (`spec/web_oracle_spec.cr`)
- Batch/DAG (sidekiq-dags): oracle 60/60 (`spec/dags_oracle_spec.cr`)
- сюита целиком — 193/193 (`crystal spec`)
- Бенч: `bench/crails_kiq` → `bench/results/203-kiq.md` (~26k jobs/s,
  в ~18× быстрее sidekiq 8.1 на том же Redis и семантике)

## Зависимости

redis (jgaskins), active_support_cr, active_job_cr (для адаптера).

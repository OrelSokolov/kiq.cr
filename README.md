# kiq.cr

A 1:1 Crystal port of **Sidekiq 8.1.7** — background job processing
with the namespace renamed `Sidekiq→Kiq` (`sidekiq_options→kiq_options`).
The Redis wire format is byte-compatible with the real Sidekiq: same
keys (`queue:<name>` LIST, `queues` SET, `schedule`/`retry`/`dead`
ZSET, `stat:processed[:date]`), same payload shape (jid hex-24,
`created_at`/`enqueued_at`). A job pushed by a Ruby sidekiq client is
executed by Kiq, and vice versa (verified by interop fixtures).
Includes the worker CLI `bin/kiq`, the Web UI (`Kiq::Web`) and
Batch/DAG pipelines. Suite: **29/29 core oracle + 7/7 unit + 94/94
web oracle + 60/60 dags oracle + 3/3 CLI** against sidekiq 8.1.7.

> ⚠️ EXPERIMENTAL FRAMEWORK, DO NOT USE IN PRODUCTION YET.

## Installation

```yaml
dependencies:
  kiq_cr:
    github: OrelSokolov/kiq.cr
```

```sh
shards install
```

Depends on `active_support.cr`, `active_job.cr` and the `redis` shard.

## Usage

```crystal
class HardJob
  include Kiq::Job               # include Kiq::Worker — alias
  kiq_options queue: "critical", retry: 5, dead: true, retry_queue: "low"

  def perform(*args)
  end
end

HardJob.perform_async(1, 2)
HardJob.perform_in(60, 1, 2)
HardJob.set(queue: "low").perform_async(1, 2)

Kiq::Client.push({class: "HardJob", queue: "default", args: [1]})
Kiq.redis { |c| c.llen("queue:default") }
```

ActiveJob adapter:

```crystal
require "kiq_cr/active_job_adapter"
ActiveJob.adapter = ActiveJob::KiqAdapter.new
```

Run workers (sidekiq-style flags):

```sh
./bin/kiq -c 10 -q critical -q default,2 -t 25 -v   # REDIS_URL for Redis
```

## Feature comparison vs sidekiq 8.1.7

### Job DSL & client

| Feature | Original (sidekiq) | Port (kiq.cr) | Status |
|---|---|---|---|
| `include Sidekiq::Job`/`Worker`, `sidekiq_options` (queue/retry/dead/retry_queue) | ✅ | `Kiq::Job` / `kiq_options`, same options | ✅ |
| `perform_async` / `perform_in` / `perform_at` / `perform_bulk` / `perform_inline` | ✅ | Same | ✅ |
| `Sidekiq::Job::Setter` (`.set(queue:, retry:, wait:, wait_until:, sync:)`) | ✅ | `Kiq::Job::Setter` | ✅ |
| `Sidekiq::Client.push`/`push_bulk` normalization, LPUSH + `queues` SADD, `at:` → ZSET `schedule` | ✅ | Same, byte-compatible payloads | ✅ |
| `Sidekiq.redis`, `configure_server`/`configure_client`/`configure` | ✅ | Same (server blocks run in the Launcher, like the original CLI) | ✅ |
| Config: redis url, concurrency, timeout, max_retries, dead_max_jobs/timeout, poll interval, strict/random/weighted queues | ✅ | Same | ✅ |
| Wire interop with Ruby sidekiq clients/workers | n/a | Verified both directions | ✅ |

### Server

| Feature | Original | Port | Status |
|---|---|---|---|
| Launcher: processors + scheduled poller + heartbeat stats flush | ✅ | Same | ✅ |
| Weighted/strict BRPOP fetch, graceful stop (`TERM`/`INT`), quiet (`TSTP`) | ✅ | Same | ✅ |
| Retry math `count⁴ + 15 + rand(10·(count+1))`, ZSET `retry`, exhaustion → morgue (`dead` ZSET, zremrangebyrank/score), death handlers, `retry: false/0` → discard | ✅ | Same | ✅ |
| Scheduled poller | Lua ZPOPBYSCORE | ZRANGEBYSCORE + win-by-ZREM (same exactly-once guarantee) | ✅ (different mechanics) |
| Stats counters `stat:processed[:date]`/`stat:failed[:date]` | ✅ | Same (5-year TTL) | ✅ |

### API, middleware, testing

| Feature | Original | Port | Status |
|---|---|---|---|
| `Sidekiq::Queue` (size/latency/clear/all), `Stats`, `ScheduledSet`/`RetrySet`/`DeadSet` | ✅ | `Kiq::` counterparts | ✅ |
| `Middleware::Chain` (add/prepend/remove/exists?/clear) + client/server chains | ✅ | Same (chain holds instances, not classes) | ✅ |
| `Sidekiq::Testing` fake!/inline!/disable!, `jobs`/`drain`/`perform_one`, `Queues` | ✅ | `Kiq::Testing` | ✅ (process-global, not thread-local) |
| ActiveJob adapter (SidekiqAdapter, JobWrapper with `wrapped`) | ✅ | `ActiveJob::KiqAdapter` | ✅ |

### Web UI (`Sidekiq::Web` → `Kiq::Web`)

| Feature | Original | Port | Status |
|---|---|---|---|
| 36 routes 1:1: dashboard, queues, retries, scheduled, dead/morgue, busy, metrics | ✅ | Same, ECR views, en/ru locales | ✅ |
| Filtering (`?substr=`), LIST pagination, CSP `script-src 'self' 'nonce-…'`, CSRF-lite (POST without `Sec-Fetch-Site` → 403), HEAD heartbeat, Location redirects | ✅ | Same (94/94 oracle markers) | ✅ |
| Mount under a prefix | ✅ | `HTTP::Server.new([Kiq::Web.new])` at root only | ❌ |
| Execution-time metrics histograms | ✅ | Counts-only (`Metrics::Tracking`) | ⚠️ partial |
| Profiles (`ProfileSet`, `/profiles/:key`) | ✅ | Stub redirect | ❌ |
| WorkSet / per-job `/busy` records | ✅ | Processes only, no per-job records | ⚠️ partial |

### Batch & DAG pipelines

| Feature | Original | Port | Status |
|---|---|---|---|
| `Sidekiq::Batch`/`Status`, callbacks, `with_redis_lock` | ✅ | `Kiq::Batch` (JSON snapshots instead of Marshal) | ✅ |
| Pipeline/Node DSL, `PipelineStatus` (sidekiq-dags) | ✅ | `Kiq::Node` + `pipeline`/`node` macros | ✅ |

### Not ported (yet)

- capsules, Redis namespace, Sidekiq Pro APIs, `sidekiq_retry_in` /
  `sidekiq_retries_exhausted` blocks, backtrace compression, per-job
  I18n locale.

## Deviations & Crystal-specific notes

- Job classes are a compile-time registry (`macro included` + arity
  dispatch from `perform`); Ruby's runtime `constantize` is impossible
  in Crystal. Unknown class → error_handlers, no retry.
- Jobs are baked in at build time: use `KIQ_REQUIRE=../app/jobs/**
  shards build kiq` or your own entry-point requiring `kiq_cr/src/kiq_cli`.
- `Kiq` writes `enqueued_at`/`created_at` as epoch-ms (sidekiq uses
  float seconds); `Queue#latency` understands both formats.
- `raise "string"` maps to error_class `RuntimeError` for wire
  compatibility; job arguments are JSON-native (`Kiq::Arg`), Symbol →
  String.

Full list with rationale: `PLAN.md` (Russian).

## Testing

```sh
crystal spec          # web/dags suites require a local Redis, else pending
```

Oracle: 29 core + 94 web + 59 dags cases recorded with the real
sidekiq 8.1.7 (`oracle/cases.rb`, `oracle/web_cases.rb`,
`oracle/dags_cases.rb` → `expected*.yml`); the Crystal specs replay
them 1:1 without edits. Vendored source of reference:
`oracle/vendor/sidekiq-8.1.7/`.

## Related

- `sidekiq_dags.cr` — companion shard with the sidekiq-dags gem port
  built on top of `kiq_cr`.

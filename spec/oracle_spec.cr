# Kiq oracle mirror: reproduces oracle/expected.yml (captured from real
# sidekiq 8.1.7 via oracle/cases.rb) 1:1 — same case IDs, same
# expectations, no edits on either side. Requires local Redis (DB 15 is
# flushed); skipped otherwise.

require "spec"
require "../src/kiq_cr"

KIQ_EXPECTED = begin
  hash = {} of String => String
  File.read("#{__DIR__}/../oracle/expected.yml").each_line do |line|
    next unless line.includes?('\t')
    id, value = line.split('\t', 2)
    hash[id] = value.chomp
  end
  hash
end

def oracle_case(id, result)
  result = result.join("\n") if result.is_a?(Array(String))
  it id do
    result.should eq KIQ_EXPECTED[id]
  end
end

REDIS_OK = begin
  ::Redis::Client.new(URI.parse("redis://127.0.0.1:6379/15")).ping
  true
rescue ex
  false
end

pending_or_run = REDIS_OK

# --- harness (mirrors oracle/cases.rb structure) ---------------------------

LOG = [] of String

KIQ_CFG = begin
  cfg = Kiq.default_configuration
  cfg.redis = "redis://127.0.0.1:6379/15"
  cfg.concurrency = 1
  cfg.timeout = 2
  cfg.poll_interval_average = 0.2
  cfg.average_scheduled_poll_interval = 0.2
  cfg.logger = ::Log.for("Kiq", :error)
  cfg.death_handlers << ->(job : Kiq::Msg, ex : Exception) { LOG << "death:#{job["class"]}" }
  cfg
end

def clean
  KIQ_CFG.redis_client.flushdb
  LOG.clear
end

def lrange(queue)
  KIQ_CFG.redis_client.lrange("queue:#{queue}", 0, -1).map(&.to_s)
end

def zpayloads(key)
  flat = KIQ_CFG.redis_client.zrange(key, 0, -1, with_scores: true).map(&.to_s)
  pairs = [] of {String, Float64}
  flat.each_slice(2) do |slice|
    pairs << {slice[0], slice[1].to_f}
  end
  pairs
end

def wait_for(timeout = 10)
  deadline = Time.utc.to_unix_f + timeout
  loop do
    return true if yield
    raise "timeout waiting for condition" if Time.utc.to_unix_f > deadline
    sleep(0.05.seconds)
  end
end

# jid (24 hex chars) and epoch-ms timestamps become placeholders so the
# mirror output is byte-comparable with the Ruby oracle (same norm).
TS_KEYS = ["created_at", "enqueued_at", "failed_at", "retried_at"]

def norm(v : JSON::Any, key = nil) : String
  case raw = v.raw
  when Hash        then "{" + raw.keys.sort.map { |k| "#{k}:#{norm(raw[k], k)}" }.join(",") + "}"
  when Array       then "[" + raw.map { |x| norm(x) }.join(",") + "]"
  when String      then v.to_s =~ /\A[0-9a-f]{24}\z/ ? "<jid>" : v.to_s
  when Int64       then TS_KEYS.includes?(key) ? "<ms>" : v.to_s
  when Bool        then v.to_s
  when Nil         then "nil"
  else             v.to_s
  end
end

def norm(v : Kiq::Msg, key = nil) : String
  norm(JSON::Any.new(v), key)
end

# --- oracle jobs ------------------------------------------------------------

class A
  include Kiq::Job
  def perform(*args)
    LOG << "A:#{args.map(&.to_s).join(",")}"
  end
end

class B
  include Kiq::Job
  kiq_options queue: "low", retry: 3
  def perform(*args)
    LOG << "B:#{args.map(&.to_s).join(",")}"
  end
end

class F1
  include Kiq::Job
  kiq_options retry: 2
  def perform(*args)
    LOG << "F1:run"
    raise "boom"
  end
end

class F2
  include Kiq::Job
  kiq_options retry: 1
  def perform(*args)
    raise "boom2"
  end
end

class F0
  include Kiq::Job
  kiq_options retry: 0
  def perform(*args)
    raise "boom0"
  end
end

class FF
  include Kiq::Job
  kiq_options retry: false
  def perform(*args)
    raise "boomf"
  end
end

class FD
  include Kiq::Job
  kiq_options retry: 2, dead: false
  def perform(*args)
    raise "boomd"
  end
end

# --- middleware --------------------------------------------------------------

class TagClient
  include Kiq::Middleware::ClientMiddleware
  def call(job_class : String, msg : Kiq::Msg, queue : String, & : -> Kiq::Msg?) : Kiq::Msg?
    msg["tag"] = JSON::Any.new("cm")
    yield
  end
end

class StopClient
  include Kiq::Middleware::ClientMiddleware
  def call(job_class : String, msg : Kiq::Msg, queue : String, & : -> Kiq::Msg?) : Kiq::Msg?
    return nil if msg["args"]?.try(&.to_s) == [JSON::Any.new(999_i64)].to_s
    yield
  end
end

class WrapServer
  include Kiq::Middleware::ServerMiddleware
  def call(job : Kiq::AnyJob, msg : Kiq::Msg, queue : String, & : -> Nil) : Nil
    LOG << "sbefore"
    yield
    LOG << "safter"
  end
end

# --- launcher driving ----------------------------------------------------------

class LauncherBox
  property launcher : Kiq::Launcher?

  def run(queues)
    stop
    @launcher = Kiq::Launcher.new(KIQ_CFG, queues, concurrency: 1, strict: true)
    @launcher.not_nil!.run
  end

  def stop
    if l = @launcher
      l.stop # flushes stats, like sidekiq's clear_heartbeat
    end
    @launcher = nil
  end
end

BOX = LauncherBox.new

if REDIS_OK
  # =========================================================================
  # 1. Client push semantics
  # =========================================================================
  clean

  jid = A.perform_async(1, "x")
  payload = Kiq.load_json(lrange("default").first)
  oracle_case("ki_push_payload", norm(payload))
  oracle_case("ki_push_jid", (!!(jid =~ /\A[0-9a-f]{24}\z/)).to_s)

  B.perform_async
  oracle_case("ki_queue_registered", KIQ_CFG.redis_client.smembers("queues").map(&.to_s).sort.join(","))

  bulk_args = [[1], [2], [3]].map { |a| JSON::Any.new(a.map { |x| JSON::Any.new(x) }) }
  jids = Kiq::Client.push_bulk({
    "class" => JSON::Any.new("A"),
    "args"  => JSON::Any.new(bulk_args),
  } of String => JSON::Any)
  oracle_case("ki_push_bulk", "llen:#{lrange("default").size} jids_ok:#{jids.all? { |j| j =~ /\A[0-9a-f]{24}\z/ }}")

  A.set(queue: "crit", retry: 7).perform_async(9)
  oracle_case("ki_setter", norm(Kiq.load_json(lrange("crit").first)))

  oracle_case("ki_options", norm(Kiq.load_json(lrange("low").first)))

  begin
    Kiq::Client.push({"args" => JSON::Any.new([] of JSON::Any)} of String => JSON::Any)
    oracle_case("ki_push_validation", "no-error")
  rescue ex : ArgumentError
    oracle_case("ki_push_validation", ex.class.name)
  end

  # =========================================================================
  # 2. Scheduling
  # =========================================================================
  clean

  A.perform_in(3600, 5)
  member, score = zpayloads("schedule").first
  payload = Kiq.load_json(member)
  delta = score - Time.utc.to_unix_f
  oracle_case("ki_perform_in", "zcard:#{KIQ_CFG.redis_client.zcard("schedule")} bucket:#{(delta / 60).round.to_i}min payload:#{norm(payload)} queue_empty:#{lrange("default").empty?}")

  A.perform_at(Time.utc - 10.seconds)
  oracle_case("ki_perform_at_past", "schedule:#{KIQ_CFG.redis_client.zcard("schedule")} queue:#{lrange("default").size}")

  # =========================================================================
  # 3. Worker processing (strict queue order, concurrency 1)
  # =========================================================================
  clean

  A.perform_async(1)
  A.perform_async(2)
  A.perform_async(3)
  B.perform_async("x")
  BOX.run(["default", "low"])
  wait_for { LOG.size == 4 }
  sleep(0.2.seconds)
  BOX.stop

  oracle_case("ki_worker_processes", LOG.join(","))
  stats = Kiq::Stats.new
  oracle_case("ki_stats", "processed:#{stats.processed} failed:#{stats.failed} enqueued:#{stats.enqueued} queues:#{stats.queues.map { |k, v| "#{k}=#{v}" }.join(",")}")

  nowdate = Time.utc.to_s("%Y-%m-%d")
  ttl = KIQ_CFG.redis_client.ttl("stat:processed:#{nowdate}")
  oracle_case("ki_stats_ttl", (ttl > 4.5 * 365 * 86400 && ttl <= 5 * 365 * 86400).to_s)

  # =========================================================================
  # 4. Retry semantics
  # =========================================================================
  clean

  F1.perform_async
  BOX.run(["default"])
  wait_for { !zpayloads("retry").empty? && LOG.includes?("F1:run") }
  BOX.stop

  member, score = zpayloads("retry").first
  rp = Kiq.load_json(member)

  delay = score - rp["failed_at"].raw.as(Int64).to_f / 1000.0
  oracle_case("ki_retry_backoff", "count:#{rp["retry_count"]} class:#{rp["error_class"]} msg:#{rp["error_message"]} queue:#{rp["queue"]} payload:#{norm(rp)} delay_ok:#{delay >= 15 && delay <= 25}")

  # promote the retry entry manually and fail again -> retry_count: 1
  member = zpayloads("retry").first[0]
  KIQ_CFG.redis_client.zrem("retry", member)
  msg = Kiq.load_json(member)
  KIQ_CFG.redis_client.lpush("queue:#{msg["queue"]}", member)
  BOX.run(["default"])
  wait_for { !zpayloads("retry").empty? }
  BOX.stop
  member2, score2 = zpayloads("retry").first
  rp2 = Kiq.load_json(member2)
  delay2 = score2 - rp2["retried_at"].raw.as(Int64).to_f / 1000.0
  oracle_case("ki_retry_second", "count:#{rp2["retry_count"]} has_retried_at:#{rp2["retried_at"]? ? true : false} delay_ok:#{delay2 >= 16 && delay2 <= 36}")

  # retry exhausted -> morgue
  clean
  F2.perform_async
  BOX.run(["default"])
  wait_for { !zpayloads("retry").empty? }
  BOX.stop
  member = zpayloads("retry").first[0]
  KIQ_CFG.redis_client.zrem("retry", member)
  msg = Kiq.load_json(member)
  KIQ_CFG.redis_client.lpush("queue:#{msg["queue"]}", member)
  BOX.run(["default"])
  wait_for { !zpayloads("dead").empty? }
  BOX.stop
  dead_payload = Kiq.load_json(zpayloads("dead").first[0])
  oracle_case("ki_retry_exhausted", "retry:#{KIQ_CFG.redis_client.zcard("retry")} dead:#{KIQ_CFG.redis_client.zcard("dead")} count:#{dead_payload["retry_count"]} class:#{dead_payload["error_class"]} log:#{LOG.join(",")}")

  # retry: 0 -> immediate death
  clean
  F0.perform_async
  BOX.run(["default"])
  wait_for { !zpayloads("dead").empty? }
  BOX.stop
  oracle_case("ki_retry_zero", "retry:#{KIQ_CFG.redis_client.zcard("retry")} dead:#{KIQ_CFG.redis_client.zcard("dead")}")

  # retry: false -> discarded, nothing stored
  clean
  FF.perform_async
  BOX.run(["default"])
  wait_for { lrange("default").empty? }
  sleep(0.3.seconds)
  BOX.stop
  oracle_case("ki_retry_false", "retry:#{KIQ_CFG.redis_client.zcard("retry")} dead:#{KIQ_CFG.redis_client.zcard("dead")} queues:#{lrange("default").size}")

  # dead: false -> discarded when exhausted (needs 3 failures with retry: 2)
  clean
  FD.perform_async
  BOX.run(["default"])
  wait_for { !zpayloads("retry").empty? }
  2.times do |i|
    member = zpayloads("retry").first[0]
    KIQ_CFG.redis_client.zrem("retry", member)
    msg = Kiq.load_json(member)
    KIQ_CFG.redis_client.lpush("queue:#{msg["queue"]}", member)
    if i.zero?
      wait_for { !zpayloads("retry").empty? }
    else
      wait_for { LOG.includes?("death:FD") }
    end
  end
  sleep(0.3.seconds)
  BOX.stop
  oracle_case("ki_dead_false", "dead:#{KIQ_CFG.redis_client.zcard("dead")} retry:#{KIQ_CFG.redis_client.zcard("retry")} log:#{LOG.sort.join(",")}")

  # =========================================================================
  # 5. Scheduled promotion via poller
  # =========================================================================
  clean

  A.perform_in(0.3, 7)
  BOX.run(["default"])
  wait_for { LOG.includes?("A:7") && KIQ_CFG.redis_client.zcard("schedule") == 0 }
  BOX.stop
  oracle_case("ki_scheduled_promote", "log:#{LOG.join(",")} schedule:#{KIQ_CFG.redis_client.zcard("schedule")}")

  # =========================================================================
  # 6. API: Queue / sets
  # =========================================================================
  clean

  A.perform_async(1)
  A.perform_async(2)
  q = Kiq::Queue.new("default")
  latency_ok = q.latency >= 0 && q.latency < 60
  oracle_case("ki_api_queue", "size:#{q.size} latency_ok:#{latency_ok} all:#{Kiq::Queue.all.map(&.name).sort.join(",")}")
  q.clear
  oracle_case("ki_api_queue_clear", "size:#{Kiq::Queue.new("default").size} sets:#{KIQ_CFG.redis_client.smembers("queues").map(&.to_s).sort.join(",")}")

  A.perform_in(3600)
  A.perform_in(7200)
  F1.perform_async
  F0.perform_async
  BOX.run(["default"])
  wait_for { !zpayloads("dead").empty? }
  BOX.stop
  sched = Kiq::ScheduledSet.new
  retry_set = Kiq::RetrySet.new
  dead = Kiq::DeadSet.new
  oracle_case("ki_api_sets", "sched:#{sched.size} retry:#{retry_set.size} dead:#{dead.size}")
  dead.clear
  sched.clear
  oracle_case("ki_api_sets_clear", "sched:#{Kiq::ScheduledSet.new.size} dead:#{Kiq::DeadSet.new.size}")

  # =========================================================================
  # 7. Testing modes
  # =========================================================================
  clean

  Kiq.testing!("fake") do
    A.perform_async(5)
    job = A.jobs.first
    oracle_case("ki_testing_fake", "jobs:#{A.jobs.size} args:#{norm(job.not_nil!["args"])} queue:#{job.not_nil!["queue"]} redis:#{lrange("default").size}")
    A.drain
    oracle_case("ki_testing_fake_drain", "jobs:#{A.jobs.size} log:#{LOG.join(",")}")
  end

  Kiq.testing!("inline") do
    A.perform_async(6)
    oracle_case("ki_testing_inline", "jobs:#{A.jobs.size} log:#{LOG.join(",")} redis:#{lrange("default").size}")
  end

  res = A.perform_inline(8)
  oracle_case("ki_perform_inline", "ret:#{res.inspect} log:#{LOG.join(",")} redis:#{lrange("default").size}")

  # =========================================================================
  # 8. Middleware
  # =========================================================================
  clean

  KIQ_CFG.client_middleware do |chain|
    chain.add TagClient.new
    chain.add StopClient.new
  end
  A.perform_async(1)
  A.perform_async(999)
  payload = Kiq.load_json(lrange("default").first)
  oracle_case("ki_middleware_client", "tag:#{payload["tag"]} stopped_llen:#{lrange("default").size}")

  KIQ_CFG.server_middleware do |chain|
    chain.add WrapServer.new
  end
  clean
  A.perform_async(1)
  BOX.run(["default"])
  wait_for { LOG.includes?("safter") }
  BOX.stop
  oracle_case("ki_middleware_server", LOG.join(","))

  KIQ_CFG.client_middleware do |chain|
    chain.remove TagClient
    chain.remove StopClient
  end
  KIQ_CFG.server_middleware do |chain|
    chain.remove WrapServer
  end

  # =========================================================================
  clean
else
  pending "kiq oracle mirror (requires local Redis on 127.0.0.1:6379)" do
    assert false
  end
end

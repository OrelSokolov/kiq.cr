# Oracle cases for the Sidekiq port "Kiq" (kiq.cr).
#
# Run against the ORIGINAL gem:
#   TZ=UTC ruby oracle/cases.rb > oracle/expected.yml
#
# Each line: "<case_id>\t<value>". The same case IDs are mirrored 1:1 in
# spec/oracle_spec.cr; the Crystal port must reproduce the file exactly
# without editing the cases. Cases capture OBSERVABLE semantics (Redis
# keys/payloads/scores, execution order, retry math, API counts), not
# sidekiq's internals.
#
# Requires: local Redis (DB 15 is flushed), sidekiq = 8.1.7, redis-client.

ENV["TZ"] = "UTC"

require "sidekiq"
require "sidekiq/api"
require "sidekiq/launcher"
require "securerandom"

raise "sidekiq 8.1.7 expected, got #{Sidekiq::VERSION}" unless Sidekiq::VERSION == "8.1.7"

URL = ENV.fetch("KIQ_ORACLE_REDIS", "redis://127.0.0.1:6379/15")

LOG = []

# NB: Sidekiq.configure_server only runs its block inside a real CLI
# process (server? checks defined?(Sidekiq::CLI)); standalone we mutate
# Sidekiq.default_configuration directly, exactly what the CLI does with
# the config blocks.
CFG = Sidekiq.default_configuration
CFG.redis = {url: URL}
CFG[:concurrency] = 1
CFG[:timeout] = 2
CFG[:poll_interval_average] = 0.2
CFG[:average_scheduled_poll_interval] = 0.2
CFG.logger = Logger.new(File::NULL)
CFG.error_handlers.clear
CFG.death_handlers << ->(job, ex) { LOG << "death:#{job["class"]}" }

Sidekiq.configure_client do |config|
  config.redis = {url: URL}
  config.logger = Logger.new(File::NULL)
  config.error_handlers.clear
end

OUT = []
def emit(id, value)
  OUT << [id, value.to_s].join("\t")
  warn "== #{id}: #{value}"
end

def clean
  Sidekiq.redis { |c| c.flushdb }
  LOG.clear
end

def redis
  Sidekiq.redis { |c| yield c }
end

# --- payload normalization -------------------------------------------------
# jid (24 hex chars) and epoch-millisecond timestamps become placeholders
# so two runs of the oracle are byte-identical.
TS_KEYS = %w[created_at enqueued_at failed_at retried_at].freeze

def norm(v, key = nil)
  case v
  when Hash then "{" + v.sort.map { |k, x| "#{k}:#{norm(x, k)}" }.join(",") + "}"
  when Array then "[" + v.map { |x| norm(x) }.join(",") + "]"
  when String then v =~ /\A[0-9a-f]{24}\z/ ? "<jid>" : v
  when Integer then TS_KEYS.include?(key) ? "<ms>" : v.to_s
  when TrueClass, FalseClass then v.to_s
  when NilClass then "nil"
  else v.to_s
  end
end

def lrange(queue)
  redis { |c| c.lrange("queue:#{queue}", 0, -1) }
end

def zpayloads(key)
  redis { |c| c.zrange(key, 0, -1, withscores: true) }
end

def wait_for(timeout = 10)
  deadline = Time.now.to_f + timeout
  loop do
    return true if yield
    raise "timeout waiting for condition" if Time.now.to_f > deadline
    sleep 0.05
  end
end

# --- oracle jobs ------------------------------------------------------------

class A
  include Sidekiq::Job
  def perform(*args)
    LOG << "A:#{args.map(&:to_s).join(",")}"
  end
end

class B
  include Sidekiq::Job
  sidekiq_options queue: "low", retry: 3
  def perform(*args)
    LOG << "B:#{args.map(&:to_s).join(",")}"
  end
end

class F1 # retry: 2, fails always
  include Sidekiq::Job
  sidekiq_options retry: 2
  def perform(*args)
    LOG << "F1:run"
    raise "boom"
  end
end

class F2 # retry: 1
  include Sidekiq::Job
  sidekiq_options retry: 1
  def perform(*args)
    raise "boom2"
  end
end

class F0 # retry: 0 -> straight to morgue
  include Sidekiq::Job
  sidekiq_options retry: 0
  def perform(*args)
    raise "boom0"
  end
end

class FF # retry: false -> discarded
  include Sidekiq::Job
  sidekiq_options retry: false
  def perform(*args)
    raise "boomf"
  end
end

class FD # retry: 2, dead: false -> discarded when exhausted
  include Sidekiq::Job
  sidekiq_options retry: 2, dead: false
  def perform(*args)
    raise "boomd"
  end
end

# --- client middleware -------------------------------------------------------

class TagClient
  include Sidekiq::ClientMiddleware
  def call(job_class, msg, queue, redis_pool)
    msg["tag"] = "cm"
    yield
  end
end

class StopClient
  include Sidekiq::ClientMiddleware
  def call(job_class, msg, queue, redis_pool)
    return nil if msg["args"] == [999]
    yield
  end
end

# --- server middleware -------------------------------------------------------

class WrapServer
  include Sidekiq::ServerMiddleware
  def call(job_instance, msg, queue)
    LOG << "sbefore"
    yield
    LOG << "safter"
  end
end

# --- launcher driving -------------------------------------------------------

@launcher = nil

def run_launcher(queues)
  stop_launcher
  cap = Sidekiq.default_configuration.default_capsule
  cap.concurrency = 1
  cap.queues = queues
  @launcher = Sidekiq::Launcher.new(Sidekiq.default_configuration)
  @launcher.run
  @launcher
end

def stop_launcher
  @launcher&.stop # flushes stats on clear_heartbeat
  @launcher = nil
end

# ============================================================================
# 1. Client push semantics
# ============================================================================
clean

jid = A.perform_async(1, "x")
payload = Sidekiq.load_json(lrange("default").first)
emit("ki_push_payload", norm(payload))
emit("ki_push_jid", (!!(jid =~ /\A[0-9a-f]{24}\z/)).to_s)

B.perform_async
emit("ki_queue_registered", redis { |c| c.smembers("queues").sort.join(",") })

jids = Sidekiq::Client.push_bulk("class" => A, "args" => [[1], [2], [3]])
emit("ki_push_bulk", "llen:#{lrange("default").size} jids_ok:#{jids.all? { |j| j =~ /\A[0-9a-f]{24}\z/ }}")

A.set(queue: "crit", retry: 7).perform_async(9)
emit("ki_setter", norm(Sidekiq.load_json(lrange("crit").first)))

emit("ki_options", norm(Sidekiq.load_json(lrange("low").first)))

begin
  Sidekiq::Client.push("args" => [])
  emit("ki_push_validation", "no-error")
rescue ArgumentError => e
  emit("ki_push_validation", e.class.name)
end

# ============================================================================
# 2. Scheduling
# ============================================================================
clean

A.perform_in(3600, 5)
(score, payload) = zpayloads("schedule").first.map(&:to_s)
  .then { |arr| [arr[1].to_f, Sidekiq.load_json(arr[0])] }
delta = score - Time.now.to_f
emit("ki_perform_in", "zcard:#{redis { |c| c.zcard("schedule") }} bucket:#{(delta / 60).round}min payload:#{norm(payload)} queue_empty:#{lrange("default").empty?}")

A.perform_at(Time.now - 10)
emit("ki_perform_at_past", "schedule:#{redis { |c| c.zcard("schedule") }} queue:#{lrange("default").size}")

# ============================================================================
# 3. Worker processing (strict queue order, concurrency 1)
# ============================================================================
clean

A.perform_async(1)
A.perform_async(2)
A.perform_async(3)
B.perform_async("x")
run_launcher(["default", "low"])
wait_for { LOG.size == 4 }
sleep 0.2 # let the processor ack
stop_launcher

emit("ki_worker_processes", LOG.join(","))
stats = Sidekiq::Stats.new
emit("ki_stats", "processed:#{stats.processed} failed:#{stats.failed} enqueued:#{stats.enqueued} queues:#{stats.queues.sort.map { |k, v| "#{k}=#{v}" }.join(",")}")

nowdate = Time.now.utc.strftime("%Y-%m-%d")
ttl = redis { |c| c.ttl("stat:processed:#{nowdate}") }
emit("ki_stats_ttl", (ttl > 4.5 * 365 * 86400 && ttl <= 5 * 365 * 86400).to_s)

# ============================================================================
# 4. Retry semantics
# ============================================================================
clean

F1.perform_async
run_launcher(["default"])
wait_for { !zpayloads("retry").empty? && LOG.include?("F1:run") }
stop_launcher

member, score = zpayloads("retry").first
rp = Sidekiq.load_json(member)

delay = score.to_f - rp["failed_at"] / 1000.0
emit("ki_retry_backoff", "count:#{rp["retry_count"]} class:#{rp["error_class"]} msg:#{rp["error_message"]} queue:#{rp["queue"]} payload:#{norm(rp)} delay_ok:#{delay >= 15 && delay <= 25}")

# promote the retry entry manually and fail again -> retry_count: 1
member = redis { |c| c.zrange("retry", 0, 0).first }
redis { |c| c.zrem("retry", member) }
msg = Sidekiq.load_json(member)
redis { |c| c.lpush("queue:#{msg["queue"]}", member) }
run_launcher(["default"])
wait_for { !zpayloads("retry").empty? }
stop_launcher
member2, score2 = zpayloads("retry").first
rp2 = Sidekiq.load_json(member2)
delay2 = score2.to_f - rp2["retried_at"] / 1000.0
emit("ki_retry_second", "count:#{rp2["retry_count"]} has_retried_at:#{rp2.key?("retried_at")} delay_ok:#{delay2 >= 16 && delay2 <= 36}")

# retry exhausted -> morgue
clean
F2.perform_async
run_launcher(["default"])
wait_for { !zpayloads("retry").empty? }
stop_launcher
member = redis { |c| c.zrange("retry", 0, 0).first }
redis { |c| c.zrem("retry", member) }
msg = Sidekiq.load_json(member)
redis { |c| c.lpush("queue:#{msg["queue"]}", member) }
run_launcher(["default"])
wait_for { !zpayloads("dead").empty? }
stop_launcher
dead_payload = Sidekiq.load_json(zpayloads("dead").first[0])
emit("ki_retry_exhausted", "retry:#{redis { |c| c.zcard("retry") }} dead:#{redis { |c| c.zcard("dead") }} count:#{dead_payload["retry_count"]} class:#{dead_payload["error_class"]} log:#{LOG.join(",")}")

# retry: 0 -> immediate death
clean
F0.perform_async
run_launcher(["default"])
wait_for { !zpayloads("dead").empty? }
stop_launcher
emit("ki_retry_zero", "retry:#{redis { |c| c.zcard("retry") }} dead:#{redis { |c| c.zcard("dead") }}")

# retry: false -> discarded, nothing stored
clean
FF.perform_async
run_launcher(["default"])
wait_for { lrange("default").empty? }
sleep 0.3
stop_launcher
emit("ki_retry_false", "retry:#{redis { |c| c.zcard("retry") }} dead:#{redis { |c| c.zcard("dead") }} queues:#{lrange("default").size}")

# dead: false -> discarded when exhausted (needs 3 failures with retry: 2)
clean
FD.perform_async
run_launcher(["default"])
wait_for { !zpayloads("retry").empty? } # failure 1 -> retry (count 0)
2.times do |i|
  member = redis { |c| c.zrange("retry", 0, 0).first }
  redis { |c| c.zrem("retry", member) }
  msg = Sidekiq.load_json(member)
  redis { |c| c.lpush("queue:#{msg["queue"]}", member) }
  if i.zero?
    wait_for { !zpayloads("retry").empty? } # failure 2 -> retry (count 1)
  else
    wait_for { LOG.join.include?("death:FD") } # failure 3 -> exhausted, discarded
  end
end
sleep 0.3
stop_launcher
emit("ki_dead_false", "dead:#{redis { |c| c.zcard("dead") }} retry:#{redis { |c| c.zcard("retry") }} log:#{LOG.sort.join(",")}")

# ============================================================================
# 5. Scheduled promotion via poller
# ============================================================================
clean

A.perform_in(0.3, 7)
run_launcher(["default"])
wait_for { LOG.include?("A:7") && redis { |c| c.zcard("schedule") } == 0 }
stop_launcher
emit("ki_scheduled_promote", "log:#{LOG.join(",")} schedule:#{redis { |c| c.zcard("schedule") }}")

# ============================================================================
# 6. API: Queue / sets
# ============================================================================
clean

A.perform_async(1)
A.perform_async(2)
q = Sidekiq::Queue.new("default")
latency_ok = q.latency >= 0 && q.latency < 60
emit("ki_api_queue", "size:#{q.size} latency_ok:#{latency_ok} all:#{Sidekiq::Queue.all.map(&:name).sort.join(",")}")
q.clear
emit("ki_api_queue_clear", "size:#{Sidekiq::Queue.new("default").size} sets:#{redis { |c| c.smembers("queues").sort.join(",") }}")

A.perform_in(3600)
A.perform_in(7200)
F1.perform_async
F0.perform_async
run_launcher(["default"])
wait_for { !zpayloads("dead").empty? }
stop_launcher
sched = Sidekiq::ScheduledSet.new
retry_set = Sidekiq::RetrySet.new
dead = Sidekiq::DeadSet.new
emit("ki_api_sets", "sched:#{sched.size} retry:#{retry_set.size} dead:#{dead.size}")
dead.clear
sched.clear
emit("ki_api_sets_clear", "sched:#{Sidekiq::ScheduledSet.new.size} dead:#{Sidekiq::DeadSet.new.size}")

# ============================================================================
# 7. Testing modes
# ============================================================================
clean

Sidekiq.testing! do
  A.perform_async(5)
  job = A.jobs.first
  emit("ki_testing_fake", "jobs:#{A.jobs.size} args:#{norm(job["args"])} queue:#{job["queue"]} redis:#{lrange("default").size}")
  A.drain
  emit("ki_testing_fake_drain", "jobs:#{A.jobs.size} log:#{LOG.join(",")}")
end

Sidekiq.testing!(:inline) do
  A.perform_async(6)
  emit("ki_testing_inline", "jobs:#{A.jobs.size} log:#{LOG.join(",")} redis:#{lrange("default").size}")
end

res = A.perform_inline(8)
emit("ki_perform_inline", "ret:#{res.inspect} log:#{LOG.join(",")} redis:#{lrange("default").size}")

# ============================================================================
# 8. Middleware
# ============================================================================
clean

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add TagClient
    chain.add StopClient
  end
end
A.perform_async(1)
A.perform_async(999)
payload = Sidekiq.load_json(lrange("default").first)
emit("ki_middleware_client", "tag:#{payload["tag"]} stopped_llen:#{lrange("default").size}")

CFG.server_middleware do |chain|
  chain.add WrapServer
end
clean
A.perform_async(1)
run_launcher(["default"])
wait_for { LOG.include?("safter") }
stop_launcher
emit("ki_middleware_server", LOG.join(","))

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.remove TagClient
    chain.remove StopClient
  end
end
CFG.server_middleware do |chain|
  chain.remove WrapServer
end

# ============================================================================
clean
OUT.each { |line| puts line }

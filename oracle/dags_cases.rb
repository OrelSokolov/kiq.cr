# Oracle cases for the sidekiq-dags port inside Kiq (kiq.cr).
#
# Run against the ORIGINAL gem (vendored in oracle/vendor/sidekiq-dags):
#   TZ=UTC ruby -I oracle/vendor/sidekiq-dags/lib oracle/dags_cases.rb > oracle/expected_dags.yml
#
# Same convention as oracle/cases.rb: "<case_id>\t<value>", mirrored 1:1 in
# spec/dags_oracle_spec.cr. Cases capture OBSERVABLE semantics (Redis keys,
# payloads, counters, callback dispatch order, pipeline statuses).
#
# Requires: local Redis (DB 15 is flushed), sidekiq = 8.1.7, sidekiq-dags
# (vendored), activesupport (for PipelineTracking).

ENV["TZ"] = "UTC"

$LOAD_PATH.unshift File.expand_path("../vendor/sidekiq-dags/lib", __dir__)

require "sidekiq"
require "sidekiq/api"
require "sidekiq/launcher"
require "securerandom"

# sidekiq-dags calls JSON.unparse, removed in json 3.x
JSON.singleton_class.send(:alias_method, :unparse, :generate) unless JSON.respond_to?(:unparse)

# PipelineStatus/Node use Time.current and present? (active_support)
require "active_support"
require "active_support/core_ext"

raise "sidekiq 8.1.7 expected, got #{Sidekiq::VERSION}" unless Sidekiq::VERSION == "8.1.7"

require "sidekiq/batch"            # sidekiq-dags core (also configures client middleware)
require "sidekiq/dummy_job"
require "sidekiq/batch_coordination_middleware"
require "sidekiq/pipeline_status"
require "sidekiq/pipeline"
require "sidekiq/pipeline_node"
require "sidekiq/pipeline_callback"
require "sidekiq/pipeline_tracking"

URL = ENV.fetch("KIQ_ORACLE_REDIS", "redis://127.0.0.1:6379/15")

LOG = []

CFG = Sidekiq.default_configuration
CFG.redis = {url: URL}
CFG[:concurrency] = 1
CFG[:timeout] = 2
CFG[:poll_interval_average] = 0.2
CFG[:average_scheduled_poll_interval] = 0.2
CFG.logger = Logger.new(File::NULL)
CFG.error_handlers.clear

Sidekiq.configure_client do |config|
  config.redis = {url: URL}
  config.logger = Logger.new(File::NULL)
  config.error_handlers.clear
end

# Standalone there is no CLI, so configure_server blocks never run —
# mutate the config directly (same effect the CLI achieves). The batch
# middleware and the coordination middleware must be present on the server.
CFG.client_middleware do |chain|
  chain.add Sidekiq::Batch::Middleware::ClientMiddleware
  chain.add Sidekiq::BatchCoordinationClientMiddleware
end
CFG.server_middleware do |chain|
  chain.add Sidekiq::BatchCoordinationServerMiddleware
  chain.add Sidekiq::Batch::Middleware::ServerMiddleware
end

OUT = []
$stdout = $stderr # gem code emits `puts` noise; keep it out of the case stream
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

def wait_for(timeout = 15)
  deadline = Time.now.to_f + timeout
  loop do
    return true if yield
    raise "timeout waiting for condition" if Time.now.to_f > deadline
    sleep 0.05
  end
end

# --- normalization -----------------------------------------------------------
# bids are urlsafe-base64(10) (16 chars), jids 24 hex chars, timestamps
# epoch floats/iso8601. We track every generated id and replace it with a
# stable placeholder in the order first seen.
IDS = {}
TS_RE = /\A\d{10}\.\d+\z/

def reg_id(v)
  return v if v.nil? || v == ""
  IDS[v] ||= "<id#{IDS.size + 1}>"
  IDS[v]
end

def norm(v)
  case v
  when Hash then "{" + v.sort.map { |k, x| "#{k}:#{norm(x)}" }.join(",") + "}"
  when Array then "[" + v.map { |x| norm(x) }.join(",") + "]"
  when String
    if IDS.key?(v) then IDS[v]
    elsif v =~ /\A[0-9a-f]{24}\z/ then reg_id(v)
    elsif v =~ /\A[0-9a-zA-Z_-]{16}\z/ && !v.include?(",") then reg_id(v)
    elsif v =~ TS_RE then "<ts>"
    else v
    end
  when Float then TS_RE.match?(v.to_s) ? "<ts>" : v.to_s
  when Integer then v.to_s
  when TrueClass, FalseClass then v.to_s
  when NilClass then "nil"
  else v.to_s
  end
end

def bidmap(h)
  "{" + h.sort.map { |k, v| "#{k}:#{norm(v)}" }.join(",") + "}"
end

def lrange(queue)
  redis { |c| c.lrange("queue:#{queue}", 0, -1) }
end

def payloads(queue)
  lrange(queue).map { |s| Sidekiq.load_json(s) }
end

# --- oracle jobs / callbacks -------------------------------------------------

class TestWorker
  include Sidekiq::Job
  def perform(*args)
    LOG << "tw:#{$bid ? "in-batch" : "no-batch"}"
  end
end

class FailWorker
  include Sidekiq::Job
  sidekiq_options retry: false
  def perform(*args)
    raise "boom"
  end
end

class LogJob
  include Sidekiq::Job
  def perform(marker)
    LOG << marker
  end
end

class WorkerA2
  include Sidekiq::Job
  def perform
    LOG << "a2"
  end
end

class WorkerB2
  include Sidekiq::Job
  def perform
    LOG << "b2"
  end
end

class WorkerC2
  include Sidekiq::Job
  def perform
    LOG << "c2"
  end
end

class SampleCallback
  def on_complete(status, opts)
    LOG << "cb:complete:total=#{status.total}:failures=#{status.failures}:id=#{opts["id"]}"
  end

  def on_success(status, opts)
    LOG << "cb:success:total=#{status.total}:failures=#{status.failures}:id=#{opts["id"]}"
  end
end

# --- launcher driving --------------------------------------------------------

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
  @launcher&.stop
  @launcher = nil
end

# ============================================================================
# 1. Batch initialization: description/callback_queue persist immediately,
#    callbacks go to per-event sets, on() rejects unknown events and
#    batches that already ran.
# ============================================================================
clean

batch = Sidekiq::Batch.new
batch.description = "describing the batch"
batch.callback_queue = "cbq"
batch.on(:complete, SampleCallback, {"id" => 42})
batch.on(:success, "SampleCallback2")
batch.on(:invalid, SampleCallback)

bid = batch.bid
reg_id(bid)
bidkey = "BID-#{bid}"

emit("dag_bid_format", (bid =~ /\A[A-Za-z0-9_-]{10,16}\z/) ? "urlsafe" : "other:#{bid.class}")
emit("dag_bid_hash_before_run", bidmap(redis { |c| c.hgetall(bidkey) }))
emit("dag_callbacks_complete", norm(redis { |c| c.smembers("#{bidkey}-callbacks-complete") }.sort))
emit("dag_callbacks_success", norm(redis { |c| c.smembers("#{bidkey}-callbacks-success") }.sort))
emit("dag_callbacks_invalid_ignored", redis { |c| c.exists("#{bidkey}-callbacks-invalid") }.to_s)
emit("dag_ttl", redis { |c| c.ttl(bidkey) }.to_s)

begin
  err = nil
  batch.add_jobs { TestWorker.perform_async }.run
  batch.on(:complete, SampleCallback)
rescue Sidekiq::Batch::BatchAlreadyStartedError
  err = "BatchAlreadyStartedError"
end
emit("dag_on_after_run", err || "no-error")

begin
  Sidekiq::Batch.new.add_jobs.run
  emit("dag_no_block", "no-error")
rescue Sidekiq::Batch::NoBlockGivenError
  emit("dag_no_block", "NoBlockGivenError")
end

# ============================================================================
# 2. Batch.run: 3 jobs -> jids, counters, queue payloads carry bid
# ============================================================================
clean

batch = Sidekiq::Batch.new
batch.on(:complete, SampleCallback, {"id" => 1})
batch.on(:success, SampleCallback, {"id" => 1})
bid = batch.bid
reg_id(bid)
bidkey = "BID-#{bid}"

jids = batch.add_jobs do
  3.times { TestWorker.perform_async }
end.run

emit("dag_run_jids", "count:#{jids.size} hex:#{jids.all? { |j| j =~ /\A[0-9a-f]{24}\z/ }}")
emit("dag_run_hash", bidmap(redis { |c| c.hgetall(bidkey) }.reject { |k, _| k == "created_at" }))
emit("dag_run_created_at_float", (redis { |c| c.hget(bidkey, "created_at") } =~ TS_RE).to_s)
emit("dag_run_jids_set", redis { |c| c.scard("#{bidkey}-jids") }.to_s)
qs = payloads("default")
emit("dag_run_queue_payloads", qs.sort_by { |p| p["args"].to_s }.map { |p|
  "{class:#{p["class"]},bid:#{p["bid"] == bid},has_jid:#{!!(p["jid"] =~ /\A[0-9a-f]{24}\z/)}}"
}.join(","))
emit("dag_run_ttl", redis { |c| c.ttl(bidkey) }.to_s)

status = Sidekiq::Batch::Status.new(bid)
emit("dag_status_data", bidmap({
  "bid" => status.bid, "total" => status.total, "failures" => status.failures,
  "pending" => status.pending, "done" => status.done, "max" => status.max,
  "complete" => status.complete?, "failure_info" => status.failure_info,
  "parent_bid" => status.parent_bid, "child_count" => status.child_count,
  "exists" => Sidekiq::Batch::ExplicitStatus.new(bid).exists?
}))

# ============================================================================
# 3. Empty batch: counters zeroed, callbacks dispatched immediately
# ============================================================================
clean

ebatch = Sidekiq::Batch.new
ebatch.callback_queue = "default"
ebatch.on(:complete, SampleCallback, {"id" => 9})
ejids = ebatch.add_jobs { nil }.run
ebid = ebatch.bid
reg_id(ebid)

emit("dag_empty_jids", ejids.size.to_s)
emit("dag_empty_hash", bidmap(redis { |c| c.hgetall("BID-#{ebid}") }.reject { |k, _| k == "created_at" }))
cbs = payloads("default").select { |p| p["class"] == "Sidekiq::Batch::Callback::Worker" }
emit("dag_empty_callback_jobs", cbs.map { |p|
  a = p["args"]
  "{clazz:#{a[0]},event:#{a[1]},opts:#{norm(a[2])},bid:#{IDS[a[3]] || "<bid>"},parent:#{a[4] ? "<bid>" : "nil"},snapshot:#{a[5].size > 20 ? "<snapshot>" : a[5]}}"
}.sort.join(","))

# ============================================================================
# 4. Server-side success path (what ServerMiddleware does per finished job)
# ============================================================================
clean

batch = Sidekiq::Batch.new
batch.on(:complete, SampleCallback, {"id" => 1})
batch.on(:success, SampleCallback, {"id" => 1})
bid = batch.bid
reg_id(bid)
jids = batch.add_jobs { 3.times { TestWorker.perform_async } }.run

# emulate the server middleware acks, in order
jids.each { |j| Sidekiq::Batch.process_successful_job(bid, j) }

h = redis { |c| c.hgetall("BID-#{bid}") }
emit("dag_success_hash", bidmap(h.reject { |k, _| k == "created_at" }))
emit("dag_success_failed_set", redis { |c| c.scard("BID-#{bid}-failed") }.to_s)
emit("dag_success_jids_set", redis { |c| c.scard("BID-#{bid}-jids") }.to_s)
cbs = payloads("default").select { |p| p["class"] == "Sidekiq::Batch::Callback::Worker" }
events = cbs.map { |p| p["args"][1] }.sort
emit("dag_success_callback_events", events.join(","))
# one arg structure, fully normalized (snapshot blob -> <snapshot>)
a = cbs.first["args"]
emit("dag_success_callback_args", "#{norm(a[0])}|#{a[1]}|opts:#{norm(a[2])}|parent:#{a[4] ? "<bid>" : "nil"}|snapshot:#{a[5].size > 20 ? "<snapshot>" : a[5]}")
# idempotency: re-running enqueue_callbacks is a no-op due to processed flags
Sidekiq::Batch.enqueue_callbacks(:complete, bid)
emit("dag_success_idempotent", payloads("default").select { |p| p["class"] == "Sidekiq::Batch::Callback::Worker" }.size.to_s)
emit("dag_processed_flag_ttl", redis { |c| c.ttl("BID-#{bid}-processed-complete") } > 0)

# ============================================================================
# 5. Server-side failure path: pending == failures -> complete, no success
# ============================================================================
clean

batch = Sidekiq::Batch.new
batch.on(:complete, SampleCallback, {"id" => 2})
batch.on(:success, SampleCallback, {"id" => 2})
bid = batch.bid
reg_id(bid)
jids = batch.add_jobs { FailWorker.perform_async }.run

jid = jids.first
Sidekiq::Batch.process_failed_job(bid, jid)

h = redis { |c| c.hgetall("BID-#{bid}") }
emit("dag_failed_hash", bidmap(h.reject { |k, _| k == "created_at" }))
emit("dag_failed_set", norm(redis { |c| c.smembers("BID-#{bid}-failed") }.sort).to_s)
cbs = payloads("default").select { |p| p["class"] == "Sidekiq::Batch::Callback::Worker" }
emit("dag_failed_callback_events", cbs.map { |p| p["args"][1] }.sort.join(","))
status = Sidekiq::Batch::Status.new(bid)
emit("dag_failed_status", "failures:#{status.failures} total:#{status.total} pending:#{status.pending}")

# ============================================================================
# 6. Nested batches: parent_bid linkage, children counters
# ============================================================================
clean

batchA = Sidekiq::Batch.new
batchB = Sidekiq::Batch.new
batchC = Sidekiq::Batch.new(batchA.bid)
batchD = Sidekiq::Batch.new
parent = batchA.bid
reg_id(parent)
children = []
alljids = []

batchB.add_jobs { alljids << WorkerB2.perform_async }
batchA.add_jobs do
  alljids << WorkerA2.perform_async
  batchB.run
  alljids << WorkerA2.perform_async
  children << batchB.bid
end
batchA.run

batchD.add_jobs { alljids << WorkerC2.perform_async }
batchC.add_jobs do
  batchD.run
  children << batchD.bid
end
batchC.run

emit("dag_nested_jids", alljids.size.to_s)
emit("dag_nested_parent_children", redis { |c| c.hget("BID-#{parent}", "children") })
children.each do |kid|
  s = Sidekiq::Batch::Status.new(kid)
  emit("dag_nested_child_#{reg_id(kid)}", "pending:#{s.pending} parent:#{s.parent_bid == parent} children:#{s.child_count}")
end
emit("dag_nested_valid", batchA.valid?.to_s)

# invalidate parent -> child invalid too
batchA.invalidate_all
emit("dag_nested_invalid", "#{batchA.valid?},#{Sidekiq::Batch.new(children.first).valid?}")

# ============================================================================
# 7. End-to-end with launcher: jobs + callbacks executed, LOG order
# ============================================================================
clean

batch = Sidekiq::Batch.new
batch.on(:complete, SampleCallback, {"id" => 5})
batch.on(:success, SampleCallback, {"id" => 5})
batch.add_jobs do
  TestWorker.perform_async
  TestWorker.perform_async
end.run

run_launcher(["default"])
wait_for { LOG.select { |l| l.start_with?("cb:") }.size == 2 }
sleep 0.3 # let callback batches finalize
stop_launcher

emit("dag_e2e_log", LOG.sort_by { |l| l.start_with?("tw") ? 0 : 1 }.join("|"))
emit("dag_e2e_hash", bidmap(redis { |c| c.hgetall("BID-#{batch.bid}") }.reject { |k, _| k == "created_at" }))
emit("dag_e2e_queue_empty", lrange("default").empty?.to_s)

# ============================================================================
# 8. End-to-end failing job in batch
# ============================================================================
clean

batch = Sidekiq::Batch.new
batch.on(:complete, SampleCallback, {"id" => 6})
batch.on(:success, SampleCallback, {"id" => 6})
batch.add_jobs do
  TestWorker.perform_async
  FailWorker.perform_async
end.run

run_launcher(["default"])
wait_for { LOG.select { |l| l.start_with?("cb:") }.size == 1 }
sleep 0.3
stop_launcher

emit("dag_e2e_fail_log", LOG.sort_by { |l| l.start_with?("cb:") ? 1 : 0 }.join("|"))
status = Sidekiq::Batch::Status.new(batch.bid)
emit("dag_e2e_fail_status", "failures:#{status.failures} total:#{status.total} done:#{status.done}")

# ============================================================================
# 9. Node chain without pipeline tracking (plain Sidekiq::Node)
# ============================================================================
clean

class ChainA < Sidekiq::Node
  desc "A"
  execute { LogJob.perform_async("chain:a") }
  next_node :ChainB
end

class ChainB < Sidekiq::Node
  desc "B"
  execute { LogJob.perform_async("chain:b") }
  next_node :ChainC
end

class ChainC < Sidekiq::Node
  desc "C"
  execute { LogJob.perform_async("chain:c") }
end

ChainA.perform_async
run_launcher(["default"])
wait_for { LOG.include?("chain:c") }
sleep 0.3
stop_launcher

emit("dag_node_chain_log", LOG.select { |l| l.start_with?("chain:") }.join("|"))
emit("dag_node_chain_batches", redis { |c| c.keys("BID-*").select { |k| k =~ /\ABID-[A-Za-z0-9_-]{16}\z/ }.size })

# single mode stops the chain
LOG.clear
clean

ChainA.perform_async("single" => true)
run_launcher(["default"])
wait_for { LOG.include?("chain:a") }
sleep 0.5
stop_launcher

emit("dag_node_single_log", LOG.select { |l| l.start_with?("chain:") }.join("|"))

# ============================================================================
# 10. Pipeline DSL with tracking (Redis-backed statuses)
# ============================================================================
clean

# The gem's own working pattern (spec/integration/pipeline_status_spec.rb):
# node classes directly inside a module, BaseNode < Sidekiq::Node with
# PipelineTracking — pipeline_name is the underscored module ("oracle_pipe").
module OraclePipe
  extend Sidekiq::NodeDsl

  class BaseNode < Sidekiq::Node
    include Sidekiq::PipelineTracking
  end

  class FirstNode < BaseNode
    desc "First"
    execute { LogJob.perform_async("pl:first") }
    next_node :SecondNode
  end

  class SecondNode < BaseNode
    desc "Second"
    execute { LogJob.perform_async("pl:second") }
    next_node :LastNode
  end

  class LastNode < BaseNode
    desc "Last"
    execute { LogJob.perform_async("pl:last") }
  end
end

PLNAME = "oracle_pipe"

Sidekiq::PipelineStatus.reset!(PLNAME)
OraclePipe::FirstNode.perform_async
run_launcher(["default"])
wait_for { LOG.include?("pl:last") }
sleep 0.5
stop_launcher

emit("dag_pipeline_log", LOG.select { |l| l.start_with?("pl:") }.join("|"))
emit("dag_pipeline_node_statuses", Sidekiq::PipelineStatus.all_node_statuses(PLNAME).sort.map { |k, v| "#{k}=#{v}" }.join(","))
emit("dag_pipeline_status", redis { |c| c.get("pipeline:#{PLNAME}:status") } || "nil")
emit("dag_pipeline_running?", Sidekiq::PipelineStatus.running?(PLNAME).to_s)
pipe = Sidekiq::SidekiqPipeline.for(PLNAME)
emit("dag_pipeline_completed?", pipe.completed?.to_s)
emit("dag_pipeline_progress", pipe.progress_percent)
emit("dag_pipeline_bid_linked", !Sidekiq::PipelineStatus.node_bid(PLNAME, "FirstNode").nil?)

# ============================================================================
# 11. PipelineStatus API surface (deterministic, no jobs)
# ============================================================================
clean

Sidekiq::PipelineStatus.start!("apx")
Sidekiq::PipelineStatus.node_start!("apx", "N1", bid: "bid-x-1")
Sidekiq::PipelineStatus.node_complete!("apx", "N1")
Sidekiq::PipelineStatus.node_start!("apx", "N2")
Sidekiq::PipelineStatus.node_fail!("apx", "N2", "err1")

emit("dag_ps_statuses", Sidekiq::PipelineStatus.all_node_statuses("apx").sort.map { |k, v| "#{k}=#{v}" }.join(","))
emit("dag_ps_node_status", Sidekiq::PipelineStatus.node_status("apx", "N1"))
emit("dag_ps_default_status", Sidekiq::PipelineStatus.node_status("apx", "Nope"))
emit("dag_ps_find_by_bid", Sidekiq::PipelineStatus.find_node_by_bid("bid-x-1").inspect)
emit("dag_ps_running_ids", Sidekiq::PipelineStatus.running_node_ids("apx").sort.join(","))
emit("dag_ps_completed_count", Sidekiq::PipelineStatus.completed_nodes_count("apx"))
emit("dag_ps_keys", redis { |c| c.keys("pipeline:apx:*") }.sort.map { |k| k.gsub("bid-x-1", "<bid>") }.join(","))
Sidekiq::PipelineStatus.finish!("apx", success: false)
emit("dag_ps_finish", Sidekiq::PipelineStatus.running?("apx").to_s)

# ============================================================================
# 12. Redis lock (with_redis_lock)
# ============================================================================
clean

seen = []
Sidekiq::Batch.with_redis_lock("lock-1", timeout: 5) do
  ttl = redis { |c| c.ttl("lock-1") }
  seen << "ttl:#{ttl > 0}"
  ok = redis { |c| c.set("lock-1", "other", nx: true, ex: 5) }
  seen << "nx-blocked:#{ok.nil?}"
  "inner"
end
seen << "released:#{redis { |c| c.exists("lock-1") }.zero?}"
emit("dag_lock", seen.join(","))

# ============================================================================
$stdout = STDOUT
if ARGV[0]
  File.write(ARGV[0], OUT.join("\n") + "\n")
else
  puts OUT.join("\n")
end
warn "TOTAL: #{OUT.size} cases"

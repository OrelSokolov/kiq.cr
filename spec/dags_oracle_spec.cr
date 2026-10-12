# Kiq dags oracle mirror: reproduces oracle/expected_dags.yml (captured
# from the real sidekiq-dags gem via oracle/dags_cases.rb) 1:1 — same case
# IDs, same expectations, no edits on either side. Requires local Redis
# (DB 15 is flushed); skipped otherwise.

require "spec"
require "../src/kiq_cr"

DAGS_EXPECTED = begin
  hash = {} of String => String
  File.read("#{__DIR__}/../oracle/expected_dags.yml").each_line do |line|
    next unless line.includes?('\t')
    id, value = line.split('\t', 2)
    hash[id] = value.chomp
  end
  hash
end

def dags_case(id, result)
  result = result.join("\n") if result.is_a?(Array(String))
  it id do
    result.should eq DAGS_EXPECTED[id]
  end
end

DAGS_REDIS_OK = begin
  ::Redis::Client.new(URI.parse("dags_redis://127.0.0.1:6379/15")).ping
  true
rescue ex
  false
end

RUN_DAGS = DAGS_REDIS_OK

DAGS_PLNAME = "oracle_pipe"

DAGS_LOG = [] of String

DAGS_CFG = begin
  cfg = Kiq.default_configuration
  cfg.redis = "dags_redis://127.0.0.1:6379/15"
  cfg.concurrency = 1
  cfg.timeout = 2
  cfg.poll_interval_average = 0.2
  cfg.average_scheduled_poll_interval = 0.2
  cfg.logger = ::Log.for("Kiq", :error)
  cfg
end

def dags_clean
  DAGS_CFG.redis_client.flushdb
  DAGS_LOG.clear
end

def dags_redis
  DAGS_CFG.redis_client
end

def dags_wait_for(timeout = 15)
  deadline = Time.utc.to_unix_f + timeout
  loop do
    return true if yield
    raise "timeout waiting for condition" if Time.utc.to_unix_f > deadline
    sleep(0.05.seconds)
  end
end

# --- normalization (mirror of oracle/dags_cases.rb) --------------------------

DAGS_IDS = {} of String => String
DAGS_TS_RE = /\A\d{10}\.\d+\z/

def dags_reg_id(v : String) : String
  DAGS_IDS[v] ||= "<id#{DAGS_IDS.size + 1}>"
end

def dags_norm(v : String) : String
  if DAGS_IDS.has_key?(v)
    DAGS_IDS[v]
  elsif v =~ /\A[0-9a-f]{24}\z/
    dags_reg_id(v)
  elsif (v =~ /\A[0-9a-zA-Z_-]{16}\z/) && !v.includes?(",")
    dags_reg_id(v)
  elsif v =~ DAGS_TS_RE
    "<ts>"
  else
    v
  end
end

def dags_norm(v : JSON::Any) : String
  dags_norm_value(v.raw).as(String)
end

def dags_norm(v : Array) : String
  "[" + v.map { |x| dags_norm(x).as(String) }.join(",") + "]"
end

def dags_norm(v : Hash) : String
  "{" + v.keys.map { |k| "#{k}:#{dags_norm(v[k]).as(String)}" }.sort.join(",") + "}"
end

def dags_norm(v : Int32 | Int64 | Float64 | Bool | Nil) : String
  if v.is_a?(Float64) && v.to_s =~ DAGS_TS_RE
    "<ts>"
  else
    v.to_s
  end
end

def dags_norm_value(raw) : String
  case raw
  when String then dags_norm(raw)
  when Array(JSON::Any) then "[" + raw.map { |x| dags_norm_value(x.raw).as(String) }.join(",") + "]"
  when Hash(String, JSON::Any) then "{" + raw.keys.map { |k| "#{k}:#{dags_norm_value(raw[k].raw).as(String)}" }.sort.join(",") + "}"
  when Nil then "nil"
  else raw.to_s
  end
end

def dags_bidmap(h : Hash)
  "{" + h.keys.map { |k| "#{k}:#{dags_norm_kv(h[k])}" }.sort.join(",") + "}"
end

def dags_norm_kv(v)
  case v
  when String then dags_norm(v)
  when Nil then "nil"
  when Array(String) then "[" + v.map { |x| dags_norm(x) }.join(",") + "]"
  when Bool, Int32, Int64, Float64 then v.to_s
  else v.to_s
  end
end

def dags_hgetall(key) : Hash(String, String)
  h = {} of String => String
  raw = dags_redis.hgetall(key)
  case raw
  when Array
    arr = raw.map(&.to_s)
    (0...(arr.size - 1)).step(2) { |i| h[arr[i]] = arr[i + 1] }
  end
  h
end

def dags_smembers(key) : Array(String)
  case v = dags_redis.smembers(key)
  when Array then v.map(&.to_s)
  else [] of String
  end
end

def dags_lrange(queue)
  dags_redis.lrange("queue:#{queue}", 0, -1).map(&.to_s)
end

def dags_payloads(queue)
  dags_lrange(queue).map { |s| Kiq.load_json(s) }
end

# --- oracle jobs / callbacks -------------------------------------------------

class TestWorker
  include Kiq::Job

  def perform(*args)
    DAGS_LOG << "tw:no-batch"
  end
end

class FailWorker
  include Kiq::Job
  kiq_options retry: false

  def perform(*args)
    raise "boom"
  end
end

class LogJob
  include Kiq::Job

  def perform(marker : ::Kiq::Arg)
    DAGS_LOG << marker.to_s
  end
end

class WorkerA2
  include Kiq::Job

  def perform
    DAGS_LOG << "a2"
  end
end

class WorkerB2
  include Kiq::Job

  def perform
    DAGS_LOG << "b2"
  end
end

class WorkerC2
  include Kiq::Job

  def perform
    DAGS_LOG << "c2"
  end
end

class SampleCallback
  include Kiq::Batch::Callback::CallbackHandler
  include Kiq::Batch::Callback::OnComplete
  include Kiq::Batch::Callback::OnSuccess

  def on_complete(status, options) : Nil
    DAGS_LOG << "cb:complete:total=#{status.total}:failures=#{status.failures}:id=#{options["id"]?.try(&.to_s)}"
  end

  def on_success(status, options) : Nil
    DAGS_LOG << "cb:success:total=#{status.total}:failures=#{status.failures}:id=#{options["id"]?.try(&.to_s)}"
  end
end

class SampleCallback2
  include Kiq::Batch::Callback::CallbackHandler
  include Kiq::Batch::Callback::OnComplete
  include Kiq::Batch::Callback::OnSuccess

  def on_complete(status, options) : Nil; end

  def on_success(status, options) : Nil; end
end
  class ChainA < Kiq::Node
  desc "A"
  execute { LogJob.perform_async("chain:a") }
  next_node :ChainB
end

class ChainB < Kiq::Node
  desc "B"
  execute { LogJob.perform_async("chain:b") }
  next_node :ChainC
end

class ChainC < Kiq::Node
  desc "C"
  execute { LogJob.perform_async("chain:c") }
end


  module OraclePipe
  class BaseNode < Kiq::Node
    include Kiq::PipelineTracking
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


# --- launcher driving --------------------------------------------------------

module DagsLauncher
  class_property launcher : Kiq::Launcher?
end

def dags_run_launcher(queues)
  dags_stop_launcher
  DAGS_CFG.queues = queues
  DAGS_CFG.queue_weights = nil
  l = Kiq::Launcher.new(DAGS_CFG, queues, 1, true)
  l.run
  DagsLauncher.launcher = l
end

def dags_stop_launcher
  DagsLauncher.launcher.try(&.stop)
  DagsLauncher.launcher = nil
end

module DagsOut
  class_property lines : Array(String) = [] of String
end

def dags_emit(id, value)
  STDERR.puts "== #{id}"
  DagsOut.lines << "#{id}\t#{value}"
end

# ============================================================================
# 1. Batch initialization
# ============================================================================
if RUN_DAGS
  dags_clean

  batch = Kiq::Batch.new
  batch.description = "describing the batch"
  batch.callback_queue = "cbq"
  batch.on(:complete, SampleCallback, {"id" => 42})
  batch.on(:success, SampleCallback2)
  batch.on(:invalid, SampleCallback)

  bid = batch.bid
  dags_reg_id(bid)
  bidkey = "BID-#{bid}"

  dags_emit("dag_bid_format", bid =~ /\A[A-Za-z0-9_-]{10,16}\z/ ? "urlsafe" : "other:#{bid.class}")
  dags_emit("dag_bid_hash_before_run", dags_bidmap(dags_hgetall(bidkey)))
  dags_emit("dag_callbacks_complete", dags_norm_arr(dags_smembers("#{bidkey}-callbacks-complete").sort))
  dags_emit("dag_callbacks_success", dags_norm_arr(dags_smembers("#{bidkey}-callbacks-success").sort))
  dags_emit("dag_callbacks_invalid_ignored", dags_redis.exists("#{bidkey}-callbacks-invalid").to_s)
  dags_emit("dag_ttl", dags_redis.ttl(bidkey).to_s)

  err = nil
  begin
    batch.add_jobs { TestWorker.perform_async }.run
    batch.on(:complete, SampleCallback)
  rescue ex : Kiq::Batch::BatchAlreadyStartedError
    err = "BatchAlreadyStartedError"
  end
  dags_emit("dag_on_after_run", err || "no-error")

  begin
    Kiq::Batch.new.add_jobs.run
    dags_emit("dag_no_block", "no-error")
  rescue ex : Kiq::Batch::NoBlockGivenError
    dags_emit("dag_no_block", "NoBlockGivenError")
  end

  # ==========================================================================
  # 2. Batch.run: 3 jobs
  # ==========================================================================
  dags_clean

  batch = Kiq::Batch.new
  batch.on(:complete, SampleCallback, {"id" => 1})
  batch.on(:success, SampleCallback, {"id" => 1})
  bid = batch.bid
  dags_reg_id(bid)
  bidkey = "BID-#{bid}"

  jids = batch.add_jobs do
    3.times { TestWorker.perform_async }
  end.run

  dags_emit("dag_run_jids", "count:#{jids.size} hex:#{jids.all? { |j| j =~ /\A[0-9a-f]{24}\z/ }}")
  h = dags_hgetall(bidkey).reject { |k, _| k == "created_at" }
  dags_emit("dag_run_hash", dags_bidmap(h))
  dags_emit("dag_run_created_at_float", (dags_hgetall(bidkey)["created_at"]? =~ DAGS_TS_RE).to_s)
  dags_emit("dag_run_jids_set", dags_redis.scard("#{bidkey}-jids").to_s)
  qs = dags_payloads("default")
  dags_emit("dag_run_queue_payloads", qs.sort_by { |p| p["args"].to_s }.map { |p|
    "{class:#{p["class"]},bid:#{p["bid"]?.try(&.to_s) == bid},has_jid:#{!!(p["jid"]?.try(&.to_s) =~ /\A[0-9a-f]{24}\z/)}}"
  }.join(","))
  dags_emit("dag_run_ttl", dags_redis.ttl(bidkey).to_s)

  status = Kiq::Batch::Status.new(bid)
  dags_emit("dag_status_data", dags_bidmap({
    "bid"          => status.bid, "total" => status.total, "failures" => status.failures,
    "pending"      => status.pending, "done" => status.done, "max" => status.max,
    "complete"     => status.complete?, "failure_info" => status.failure_info,
    "parent_bid"   => status.parent_bid, "child_count" => status.child_count,
    "exists"       => Kiq::Batch::ExplicitStatus.new(bid).exists?,
  } of String => ::Kiq::Arg | Array(String) | Bool | Nil))

  # ==========================================================================
  # 3. Empty batch with callbacks
  # ==========================================================================
  dags_clean

  ebatch = Kiq::Batch.new
  ebatch.callback_queue = "default"
  ebatch.on(:complete, SampleCallback, {"id" => 9})
  ejids = ebatch.add_jobs { nil }.run
  ebid = ebatch.bid
  dags_reg_id(ebid)

  dags_emit("dag_empty_jids", ejids.size.to_s)
  dags_emit("dag_empty_hash", dags_bidmap(dags_hgetall("BID-#{ebid}").reject { |k, _| k == "created_at" }))
  cbs = dags_payloads("default").select { |p| p["class"].to_s == "Kiq::Batch::Callback::Worker" }
  dags_emit("dag_empty_callback_jobs", cbs.map { |p|
    a = p["args"].as_a
    "{clazz:#{a[0]},event:#{a[1]},opts:#{dags_norm(::Kiq::Client.from_any(a[2]))},bid:#{DAGS_IDS[a[3].to_s]? || "<bid>"},parent:#{a[4].raw.nil? ? "nil" : "<bid>"},snapshot:#{a[5].to_s.size > 20 ? "<snapshot>" : a[5]}}"
  }.sort.join(","))

  # ==========================================================================
  # 4. Server-side success path
  # ==========================================================================
  dags_clean

  batch = Kiq::Batch.new
  batch.on(:complete, SampleCallback, {"id" => 1})
  batch.on(:success, SampleCallback, {"id" => 1})
  bid = batch.bid
  dags_reg_id(bid)
  jids = batch.add_jobs { 3.times { TestWorker.perform_async } }.run

  jids.each { |j| Kiq::Batch.process_successful_job(bid, j) }

  h = dags_hgetall("BID-#{bid}")
  dags_emit("dag_success_hash", dags_bidmap(h.reject { |k, _| k == "created_at" }))
  dags_emit("dag_success_failed_set", dags_redis.scard("BID-#{bid}-failed").to_s)
  dags_emit("dag_success_jids_set", dags_redis.scard("BID-#{bid}-jids").to_s)
  cbs = dags_payloads("default").select { |p| p["class"].to_s == "Kiq::Batch::Callback::Worker" }
  events = cbs.map { |p| p["args"].as_a[1].to_s }.sort
  dags_emit("dag_success_callback_events", events.join(","))
  a = cbs.first["args"].as_a
  dags_emit("dag_success_callback_args", "#{a[0]}|#{a[1]}|opts:#{dags_norm(::Kiq::Client.from_any(a[2]))}|parent:#{a[4].raw.nil? ? "nil" : "<bid>"}|snapshot:#{a[5].to_s.size > 20 ? "<snapshot>" : a[5]}")
  Kiq::Batch.enqueue_callbacks(:complete, bid)
  dags_emit("dag_success_idempotent", dags_payloads("default").count { |p| p["class"].to_s == "Kiq::Batch::Callback::Worker" }.to_s)
  dags_emit("dag_processed_flag_ttl", (dags_redis.ttl("BID-#{bid}-processed-complete") > 0).to_s)

  # ==========================================================================
  # 5. Server-side failure path
  # ==========================================================================
  dags_clean

  batch = Kiq::Batch.new
  batch.on(:complete, SampleCallback, {"id" => 2})
  batch.on(:success, SampleCallback, {"id" => 2})
  bid = batch.bid
  dags_reg_id(bid)
  jids = batch.add_jobs { FailWorker.perform_async }.run

  jid = jids.first
  Kiq::Batch.process_failed_job(bid, jid)

  h = dags_hgetall("BID-#{bid}")
  dags_emit("dag_failed_hash", dags_bidmap(h.reject { |k, _| k == "created_at" }))
  dags_emit("dag_failed_set", dags_norm_arr(dags_smembers("BID-#{bid}-failed").sort))
  cbs = dags_payloads("default").select { |p| p["class"].to_s == "Kiq::Batch::Callback::Worker" }
  dags_emit("dag_failed_callback_events", cbs.map { |p| p["args"].as_a[1].to_s }.sort.join(","))
  status = Kiq::Batch::Status.new(bid)
  dags_emit("dag_failed_status", "failures:#{status.failures} total:#{status.total} pending:#{status.pending}")

  # ==========================================================================
  # 6. Nested batches
  # ==========================================================================
  dags_clean

  batchA = Kiq::Batch.new
  batchB = Kiq::Batch.new
  batchC = Kiq::Batch.new(batchA.bid)
  batchD = Kiq::Batch.new
  parent = batchA.bid
  dags_reg_id(parent)
  children = [] of String
  alljids = [] of String

  batchB.add_jobs { alljids << WorkerB2.perform_async.not_nil! }
  batchA.add_jobs do
    alljids << WorkerA2.perform_async.not_nil!
    batchB.run
    alljids << WorkerA2.perform_async.not_nil!
    children << batchB.bid
  end
  batchA.run

  batchD.add_jobs { alljids << WorkerC2.perform_async.not_nil! }
  batchC.add_jobs do
    batchD.run
    children << batchD.bid
  end
  batchC.run

  dags_emit("dag_nested_jids", alljids.size.to_s)
  dags_emit("dag_nested_parent_children", dags_hgetall("BID-#{parent}")["children"]?.to_s)
  children.each do |kid|
    dags_reg_id(kid)
    s = Kiq::Batch::Status.new(kid)
    dags_emit("dag_nested_child_#{DAGS_IDS[kid]}", "pending:#{s.pending} parent:#{s.parent_bid == parent} children:#{s.child_count}")
  end
  dags_emit("dag_nested_valid", batchA.valid?.to_s)

  batchA.invalidate_all
  dags_emit("dag_nested_invalid", "#{batchA.valid?},#{Kiq::Batch.new(children.first).valid?}")

  # ==========================================================================
  # 7. End-to-end with launcher
  # ==========================================================================
  dags_clean

  batch = Kiq::Batch.new
  batch.on(:complete, SampleCallback, {"id" => 5})
  batch.on(:success, SampleCallback, {"id" => 5})
  bid = batch.bid
  batch.add_jobs do
    TestWorker.perform_async
    TestWorker.perform_async
  end.run

  dags_run_launcher(["default"])
  dags_wait_for { DAGS_LOG.count { |l| l.starts_with?("cb:") } == 2 }
  sleep(0.3.seconds)
  dags_stop_launcher

  dags_emit("dag_e2e_log", DAGS_LOG.sort_by { |l| l.starts_with?("tw") ? 0 : 1 }.join("|"))
  dags_emit("dag_e2e_hash", dags_bidmap(dags_hgetall("BID-#{bid}").reject { |k, _| k == "created_at" }))
  dags_emit("dag_e2e_queue_empty", dags_lrange("default").empty?.to_s)

  # ==========================================================================
  # 8. End-to-end failing job
  # ==========================================================================
  dags_clean

  batch = Kiq::Batch.new
  batch.on(:complete, SampleCallback, {"id" => 6})
  batch.on(:success, SampleCallback, {"id" => 6})
  bid = batch.bid
  batch.add_jobs do
    TestWorker.perform_async
    FailWorker.perform_async
  end.run

  dags_run_launcher(["default"])
  dags_wait_for { DAGS_LOG.count { |l| l.starts_with?("cb:") } == 1 }
  sleep(0.3.seconds)
  dags_stop_launcher

  dags_emit("dag_e2e_fail_log", DAGS_LOG.sort_by { |l| l.starts_with?("cb:") ? 1 : 0 }.join("|"))
  status = Kiq::Batch::Status.new(bid)
  dags_emit("dag_e2e_fail_status", "failures:#{status.failures} total:#{status.total} done:#{status.done}")

  # ==========================================================================
  # 9. Node chain without pipeline tracking
  # ==========================================================================
  dags_clean

  ChainA.perform_async
  dags_run_launcher(["default"])
  dags_wait_for { DAGS_LOG.includes?("chain:c") }
  sleep(0.3.seconds)
  dags_stop_launcher

  dags_emit("dag_node_chain_log", DAGS_LOG.select { |l| l.starts_with?("chain:") }.join("|"))
  dags_emit("dag_node_chain_batches", dags_redis.keys("BID-*").select { |k| k =~ /\ABID-[A-Za-z0-9_-]{16}\z/ }.size.to_s)

  DAGS_LOG.clear
  dags_clean

  ChainA.perform_async({"single" => true} of String => Kiq::Arg)
  dags_run_launcher(["default"])
  dags_wait_for { DAGS_LOG.includes?("chain:a") }
  sleep(0.5.seconds)
  dags_stop_launcher

  dags_emit("dag_node_single_log", DAGS_LOG.select { |l| l.starts_with?("chain:") }.join("|"))

  # ==========================================================================
  # 10. Pipeline with tracking (module pattern from the gem's own specs)
  # ==========================================================================
  dags_clean

  Kiq::PipelineStatus.reset!(DAGS_PLNAME)
  OraclePipe::FirstNode.perform_async
  dags_run_launcher(["default"])
  dags_wait_for { DAGS_LOG.includes?("pl:last") }
  sleep(0.5.seconds)
  dags_stop_launcher

  dags_emit("dag_pipeline_log", DAGS_LOG.select { |l| l.starts_with?("pl:") }.join("|"))
  statuses = Kiq::PipelineStatus.all_node_statuses(DAGS_PLNAME)
  dags_emit("dag_pipeline_node_statuses", statuses.keys.map { |k| "#{k}=#{statuses[k]}" }.sort.join(","))
  dags_emit("dag_pipeline_status", dags_redis.get("pipeline:#{DAGS_PLNAME}:status").try(&.to_s) || "nil")
  dags_emit("dag_pipeline_running?", Kiq::PipelineStatus.running?(DAGS_PLNAME).to_s)
  pipe = Kiq::Pipeline.for(DAGS_PLNAME)
  dags_emit("dag_pipeline_completed?", pipe.completed?.to_s)
  dags_emit("dag_pipeline_progress", pipe.progress_percent.to_s)
  dags_emit("dag_pipeline_bid_linked", (!Kiq::PipelineStatus.node_bid(DAGS_PLNAME, "FirstNode").nil?).to_s)

  # ==========================================================================
  # 11. PipelineStatus API surface
  # ==========================================================================
  dags_clean

  Kiq::PipelineStatus.start!("apx")
  Kiq::PipelineStatus.node_start!("apx", "N1", bid: "bid-x-1")
  Kiq::PipelineStatus.node_complete!("apx", "N1")
  Kiq::PipelineStatus.node_start!("apx", "N2")
  Kiq::PipelineStatus.node_fail!("apx", "N2", "err1")

  statuses = Kiq::PipelineStatus.all_node_statuses("apx")
  dags_emit("dag_ps_statuses", statuses.keys.map { |k| "#{k}=#{statuses[k]}" }.sort.join(","))
  dags_emit("dag_ps_node_status", Kiq::PipelineStatus.node_status("apx", "N1"))
  dags_emit("dag_ps_default_status", Kiq::PipelineStatus.node_status("apx", "Nope"))
  dags_emit("dag_ps_find_by_bid", dags_ruby_inspect_hash(Kiq::PipelineStatus.find_node_by_bid("bid-x-1")))
  dags_emit("dag_ps_running_ids", Kiq::PipelineStatus.running_node_ids("apx").sort.join(","))
  dags_emit("dag_ps_completed_count", Kiq::PipelineStatus.completed_nodes_count("apx").to_s)
  dags_emit("dag_ps_keys", dags_keys_arr("pipeline:apx:*").map { |k| k.gsub("bid-x-1", "<bid>") }.sort.join(","))
  Kiq::PipelineStatus.finish!("apx", success: false)
  dags_emit("dag_ps_finish", Kiq::PipelineStatus.running?("apx").to_s)

  # ==========================================================================
  # 12. Redis lock
  # ==========================================================================
  dags_clean

  seen = [] of String
  Kiq::Batch.with_redis_lock("lock-1", timeout: 5) do
    ttl = dags_redis.ttl("lock-1")
    seen << "ttl:#{ttl > 0}"
    ok = dags_redis.set("lock-1", "other", nx: true, ex: 5)
    seen << "nx-blocked:#{ok.nil?}"
  end
  seen << "released:#{dags_redis.exists("lock-1") == 0}"
  dags_emit("dag_lock", seen.join(","))
end

# --- helpers -----------------------------------------------------------------

def dags_norm_arr(arr : Array(String))
  "[" + arr.map { |x| dags_norm(x) }.join(",") + "]"
end

def dags_keys_arr(pattern)
  case v = dags_redis.keys(pattern)
  when Array then v.map(&.to_s)
  else [] of String
  end
end

# Ruby's Array#to_s / inspect for [String, String, Hash] values.
def dags_ruby_inspect(arr : Array)
  "[" + arr.map { |v|
    case v
    when String then v.inspect
    when Hash(String, ::Kiq::Arg) then "{" + v.keys.sort.map { |k| "#{k.inspect}=>#{dags_ruby_arg_inspect(v[k])}" }.join(",") + "}"
    else v.to_s
    end
  }.join(", ") + "]"
end

def dags_ruby_arg_inspect(v)
  case v
  when String then v.inspect
  when Nil then "nil"
  else v.to_s
  end
end

def dags_ruby_inspect_hash(h : Hash(String, String)?)
  return "nil" unless h
  "{" + h.keys.map { |k| "#{k}: #{h[k].inspect}" }.join(", ") + "}"
end

# --- run ---------------------------------------------------------------------

describe "sidekiq-dags oracle" do
  DagsOut.lines.each do |line|
    id, value = line.split('\t', 2)
    it id do
      value.should eq DAGS_EXPECTED[id]
    end
  end

  it "covers every oracle case" do
    DagsOut.lines.size.should eq DAGS_EXPECTED.size
    DagsOut.lines.map { |l| l.split('\t', 2)[0] }.should eq DAGS_EXPECTED.keys
  end
end

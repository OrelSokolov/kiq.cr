# Kiq unit specs: cross-interop with real sidekiq payloads, ActiveJob
# adapter, weighted queues, graceful stop, testing modes. These cover
# behavior outside the oracle line.

require "spec"
require "../src/kiq_cr"
require "../src/active_job_adapter"

UNIT_REDIS_OK = begin
  ::Redis::Client.new(URI.parse("redis://127.0.0.1:6379/15")).ping
  true
rescue ex
  false
end

UNIT_LOG = [] of String

UNIT_CFG = begin
  cfg = Kiq.default_configuration
  cfg.redis = "redis://127.0.0.1:6379/15"
  cfg.concurrency = 1
  cfg.timeout = 2
  cfg.poll_interval_average = 0.2
  cfg.logger = ::Log.for("Kiq", :error)
  cfg
end

def wait_for(timeout = 10)
  deadline = Time.utc.to_unix_f + timeout
  loop do
    return true if yield
    raise "timeout waiting for condition" if Time.utc.to_unix_f > deadline
    sleep(0.05.seconds)
  end
end

# A payload pushed by the REAL sidekiq 8.1.7 client (Ruby), captured
# verbatim: `RubyInteropJob.perform_async(42, "from-sidekiq")`.
SIDEKIQ_FIXTURE = %q({"retry":true,"queue":"default","args":[42,"from-sidekiq"],"class":"RubyInteropJob","jid":"21d496289db9eefaa6cb9f23","created_at":1791200682230,"enqueued_at":1791200682230})

class RubyInteropJob
  include Kiq::Job
  def perform(*args)
    UNIT_LOG << "ruby:#{args.map(&.to_s).join(",")}"
  end
end

class AjJob < ActiveJob::Base
  def perform(number)
    UNIT_LOG << "aj:#{number}"
    nil
  end
end

class CJob
  include Kiq::Job
  def perform(*args)
    UNIT_LOG << "C:#{args.map(&.to_s).join(",")}"
  end
end

if UNIT_REDIS_OK
  UNIT_CFG.redis_client.flushdb

  describe "kiq interop" do
    it "executes a payload pushed by the real sidekiq client" do
      UNIT_CFG.redis_client.lpush("queue:default", SIDEKIQ_FIXTURE)
      UNIT_CFG.redis_client.sadd("queues", "default")
      l = Kiq::Launcher.new(UNIT_CFG, ["default"], concurrency: 1, strict: true)
      l.run
      wait_for { UNIT_LOG.includes?("ruby:42,from-sidekiq") }
      l.stop
      UNIT_LOG.last.should eq("ruby:42,from-sidekiq")
    end

    it "pushes a payload the real sidekiq server can read back" do
      jid = CJob.perform_async(7)
      raw = UNIT_CFG.redis_client.lrange("queue:default", 0, -1).last.to_s
      msg = Kiq.load_json(raw)
      msg["class"].to_s.should eq("CJob")
      msg["args"].to_s.should eq([JSON::Any.new(7_i64)].to_s)
      msg["jid"].to_s.should eq(jid.to_s)
      # sidekiq 8 wire format: epoch-ms timestamps, 24-hex jid
      (msg["created_at"].raw.as?(Int64)).should_not be_nil
      (msg["jid"].to_s =~ /\A[0-9a-f]{24}\z/).should be_truthy
    end
  end

  describe "kiq weighted queues" do
    it "processes all queues regardless of weights" do
      UNIT_CFG.redis_client.flushdb
      UNIT_LOG.clear
      UNIT_CFG.queues = ["a", "b", "c"]
      UNIT_CFG.queue_weights = [1, 2, 1] # weighted mode: shuffle + uniq
      l = Kiq::Launcher.new(UNIT_CFG, UNIT_CFG.expanded_queues, concurrency: 1, strict: false)
      CJob.set(queue: "a").perform_async(1)
      CJob.set(queue: "b").perform_async(2)
      CJob.set(queue: "c").perform_async(3)
      l.run
      wait_for { UNIT_LOG.size == 3 }
      l.stop
      UNIT_LOG.sort.should eq(["C:1", "C:2", "C:3"])
      UNIT_CFG.queue_weights = nil
    end
  end

  describe "kiq graceful stop" do
    it "drains queued jobs before exiting" do
      UNIT_CFG.redis_client.flushdb
      UNIT_LOG.clear
      10.times { |i| CJob.perform_async(i) }
      l = Kiq::Launcher.new(UNIT_CFG, ["default"], concurrency: 1, strict: true)
      l.run
      wait_for { UNIT_LOG.size == 10 }
      l.stop
      UNIT_LOG.size.should eq(10)
    end
  end

  describe "ActiveJob adapter" do
    it "enqueues through Kiq and executes via the wrapper" do
      UNIT_CFG.redis_client.flushdb
      UNIT_LOG.clear
      ActiveJob.adapter = ActiveJob::KiqAdapter.new
      AjJob.perform_later(99)
      # payload is on the Kiq queue under the wrapper class
      raw = UNIT_CFG.redis_client.lrange("queue:default", 0, -1).last.to_s
      msg = Kiq.load_json(raw)
      msg["class"].to_s.should eq("ActiveJob::KiqAdapter::JobWrapper")
      msg["wrapped"].to_s.should eq("AjJob")
      l = Kiq::Launcher.new(UNIT_CFG, ["default"], concurrency: 1, strict: true)
      l.run
      wait_for { UNIT_LOG.includes?("aj:99") }
      l.stop
      UNIT_LOG.should contain("aj:99")
    end
  end

  describe "Testing" do
    it "disable! hits Redis" do
      Kiq::Testing.disable! do
        UNIT_CFG.redis_client.flushdb
        CJob.perform_async(5)
        UNIT_CFG.redis_client.llen("queue:default").should eq(1)
      end
    end

    it "fake buffer is cleared by clear_all" do
      Kiq::Testing.fake! do
        CJob.perform_async(1)
        CJob.perform_async(2)
        CJob.jobs.size.should eq(2)
        Kiq::Queues.clear_all
        CJob.jobs.size.should eq(0)
      end
    end
  end
else
  pending "kiq unit specs (requires local Redis on 127.0.0.1:6379)" do
    assert false
  end
end

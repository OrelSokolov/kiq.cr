# Kiq CLI spec: drives the real `bin/kiq` binary end to end (built via
# `rake build:kiq` / `shards build kiq`). Skipped when the binary is not
# built — the suite must stay green on plain `crystal spec` without a
# release build.

require "spec"
require "redis"
require "../src/kiq_cr"

KIQ_BIN = "#{__DIR__}/../bin/kiq"
KIQ_REDIS_URL = "redis://127.0.0.1:6379/15"

CLI_REDIS_OK = begin
  ::Redis::Client.new(URI.parse(KIQ_REDIS_URL)).ping
  true
rescue ex
  false
end

module CliHelpers
  def self.redis : ::Redis::Client
    @@redis ||= ::Redis::Client.new(URI.parse(KIQ_REDIS_URL))
  end
end

def wait_until(timeout = 10)
  deadline = Time.utc.to_unix_f + timeout
  loop do
    return true if yield
    raise "timeout" if Time.utc.to_unix_f > deadline
    sleep(0.05.seconds)
  end
end

def run_kiq(*args : String)
  Process.new(KIQ_BIN, args.to_a, env: {"REDIS_URL" => KIQ_REDIS_URL},
    output: :pipe, error: :inherit)
end

if File.exists?(KIQ_BIN) && CLI_REDIS_OK
  describe "kiq CLI" do
    it "-V prints the version" do
      io = IO::Memory.new
      Process.run(KIQ_BIN, ["-V"], output: io)
      io.to_s.strip.should eq("Kiq #{Kiq::VERSION}")
    end

    it "fails fast on unreachable Redis" do
      status = Process.run(KIQ_BIN, ["-c", "1"], env: {"REDIS_URL" => "redis://127.0.0.1:6399/0"},
        output: :pipe, error: :pipe)
      status.exit_code.should eq(1)
    end

    it "drains an unknown-class job without dying and stops gracefully on TERM" do
      CliHelpers.redis.flushdb
      worker = run_kiq("-c", "1", "-q", "default", "-t", "2")
      wait_until { true } # let it boot
      CliHelpers.redis.sadd("queues", "default")
      CliHelpers.redis.lpush("queue:default",
        %q({"retry":false,"queue":"default","args":[],"class":"NoSuchJob","jid":"aaaabbbbccccddddeeee2222","created_at":1791200000000,"enqueued_at":1791200000000}))
      wait_until { CliHelpers.redis.llen("queue:default") == 0 }
      worker.signal(Signal::TERM)
      wait_until { !worker.exists? }
      worker.wait.success?.should be_true
      # stats were flushed on stop; the unknown-class job was discarded
      CliHelpers.redis.get("stat:processed").to_s.to_i64.should be >= 1
      CliHelpers.redis.zcard("retry").should eq(0)
      CliHelpers.redis.zcard("dead").should eq(0)
    end
  end
else
  pending "kiq CLI spec (build bin/kiq first: shards build kiq; requires local Redis)" do
    assert false
  end
end

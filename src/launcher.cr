# Kiq::Launcher — starts processor fibers + the scheduled poller,
# flushes stats and maintains the process heartbeat (`processes` SET +
# `<identity>` hash), 1:1 with Sidekiq::Launcher's observable Redis
# structures (used by Kiq::Web's Busy page).

module Kiq
  class Launcher
    getter config : Config
    getter poller : Scheduled::Poller
    getter processors : Array(Processor) = [] of Processor
    getter identity : String

    HEARTBEAT_INTERVAL = 5
    HEARTBEAT_TTL = 60

    @done = false
    @quiet = false
    @started_at = Time.utc.to_unix_f

    def initialize(@config : Config, queues : Array(String)? = nil,
                   concurrency : Int32? = nil, strict : Bool? = nil)
      @queues = queues || @config.queues
      @concurrency = concurrency || @config.concurrency
      @strict = strict || (@config.mode == :strict)
      @poller = Scheduled::Poller.new(@config)
      @identity = "#{System.hostname}:#{::Process.pid}:#{Random::Secure.hex(6)}"
    end

    def run : Nil
      # The CLI executes deferred configure_server blocks at boot.
      Kiq::SERVER_BLOCKS.each(&.call(@config)) unless Kiq.server?
      Kiq.server = true

      register_process
      @poller.start
      @concurrency.times do
        processor = Processor.new(@config, @queues, @strict)
        processor.start
        @processors << processor
      end
      spawn(name: "kiq/heartbeat") do
        until @done
          sleep(HEARTBEAT_INTERVAL.seconds)
          break if @done
          flush_stats
          beat
        end
      end
      Fiber.yield
    end

    def stopping? : Bool
      @done
    end

    def quiet : Nil
      return if @done
      @quiet = true
      @done = true
      @processors.each(&.terminate)
      @poller.terminate
    end

    # Shut down, waiting for processors to exit (they leave BRPOP within
    # one 2s timeout), then flush stats and clear the heartbeat.
    def stop : Nil
      quiet
      deadline = Time.instant + (@config.timeout + 3).seconds
      while @processors.any? { |p| !p.finished? } && Time.instant < deadline
        sleep(0.05.seconds)
      end
      flush_stats
      clear_heartbeat
    end

    def flush_stats : Nil
      fails = Processor::FAILURE.reset
      procd = Processor::PROCESSED.reset
      return if fails + procd == 0

      nowdate = Time.utc.to_s("%Y-%m-%d")
      redis = @config.redis_client
      redis.incrby("stat:processed", procd)
      redis.incrby("stat:processed:#{nowdate}", procd)
      redis.expire("stat:processed:#{nowdate}", Config::STATS_TTL)
      redis.incrby("stat:failed", fails)
      redis.incrby("stat:failed:#{nowdate}", fails)
      redis.expire("stat:failed:#{nowdate}", Config::STATS_TTL)
    end

    # --- process heartbeat (sidekiq's ❤ flow, same Redis structures) ----

    def info_payload : String
      weights = {} of String => Int32
      @queues.each { |q| weights[q] = 0 }
      {
        "hostname"    => System.hostname,
        "started_at"  => @started_at,
        "pid"         => ::Process.pid,
        "tag"         => ENV["KIQ_TAG"]? || "",
        "concurrency" => @concurrency,
        "queues"      => @queues,
        "weights"     => weights,
        "labels"      => [] of String,
        "identity"    => @identity,
        "embedded"    => false,
      }.to_json
    end

    def register_process : Nil
      cleanup_dead_processes
      redis.sadd("processes", @identity)
      write_heartbeat
    end

    def write_heartbeat : Nil
      redis.hset(@identity, {
        "info"        => info_payload,
        "beat"        => Time.utc.to_unix_f.to_s,
        "busy"        => @processors.count { |p| !p.finished? && !@done }.to_s,
        "quiet"       => @quiet.to_s,
        "rss"         => rss_kb.to_s,
        "rtt_us"      => "100",
        "concurrency" => @concurrency.to_s,
      })
      redis.expire(@identity, HEARTBEAT_TTL)
    end

    def beat : Nil
      write_heartbeat
      process_signals
    end

    # The Web UI pushes TSTP/TERM to `<identity>-signals` (1:1 with
    # sidekiq's Process#quiet!/stop!).
    def process_signals : Nil
      loop do
        sig = redis.rpop("#{@identity}-signals").try(&.to_s)
        break unless sig
        case sig
        when "TSTP" then quiet unless @done
        when "TERM" then quiet unless @done # graceful, like the CLI
        end
      end
    end

    def clear_heartbeat : Nil
      redis.srem("processes", [@identity])
      redis.del(@identity)
      redis.del("#{@identity}-signals")
    end

    def cleanup_dead_processes : Nil
      redis = @config.redis_client
      return unless redis.set("process_cleanup", "1", ex: 60, nx: true)

      procs = case v = redis.smembers("processes")
              when Array then v.map(&.to_s)
              else [] of String
              end
      return if procs.empty?

      dead = procs.select { |key| redis.hget(key, "info").nil? }
      redis.srem("processes", dead) unless dead.empty?
    end

    private def rss_kb : Int64
      status_file = "/proc/#{::Process.pid}/status"
      if File.exists?(status_file)
        File.each_line(status_file) do |line|
          if line.starts_with?("VmRSS:")
            kb = line.split[1].to_i64?
            return kb || 0_i64
          end
        end
      end
      0_i64
    end

    private def redis
      @config.redis_client
    end
  end
end

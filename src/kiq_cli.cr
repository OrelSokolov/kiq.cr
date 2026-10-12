# kiq — the worker CLI binary (analogue of the `sidekiq` command).
#
# Usage:
#   shards build kiq                        # bin/kiq, no app jobs baked in
#   KIQ_REQUIRE=./app/jobs/** shards build kiq   # with your job classes
#   ./bin/kiq -c 10 -q critical -q default,2
#
# NB: Crystal has no runtime `require`, so unlike Ruby's
# `sidekiq -r ./boot.rb` job classes must be baked into the binary at
# build time via the KIQ_REQUIRE compile-time env var (a file or a
# directory glob). The runtime `-r` flag exists for compatibility and
# warns.

require "option_parser"
require "./kiq_cr"

{% if req = env("KIQ_REQUIRE") %}
  # Job classes baked at build time. NB: Crystal requires are
  # compile-time and "./"-relative to THIS file, so the path is relative
  # to the shard's src/ directory, e.g. from the shard root:
  #   KIQ_REQUIRE=../app/jobs/** shards build kiq
  # The usual pattern for an app is its own entry point instead:
  #   require "./app/jobs/**"; require "kiq_cr/src/kiq_cli"
  require {{ "./" + req }}
{% end %}

module Kiq
  class CLI
    @concurrency : Int32 = 5
    @timeout : Int32 = 25
    @queues = [] of String
    @weights = [] of Int32
    @verbose = false
    @ignored_requires = [] of String

    def run(argv = ARGV) : Nil
      parse(argv)

      config = Kiq.default_configuration
      config.redis = ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0")
      config.concurrency = @concurrency
      config.timeout = @timeout
      config.logger = ::Log.for("Kiq", @verbose ? ::Log::Severity::Debug : ::Log::Severity::Info)
      if @queues.empty?
        @queues = ["default"]
        @weights = [1]
      end
      config.queues = @queues
      config.queue_weights = @weights

      puts banner(config)

      # fail fast on an unreachable Redis, like sidekiq at boot
      begin
        config.redis_client.ping
      rescue ex : Exception
        STDERR.puts "Error connecting to Redis on #{config.redis_url}: #{ex.message}"
        exit 1
      end

      {% if ::Kiq::JOBS.empty? %}
        puts "WARNING: no job classes compiled into this binary — every job " \
             "will fail dispatch. Rebuild with KIQ_REQUIRE=<jobs.cr|./app/jobs/**>"
      {% else %}
        puts "Loaded #{ {{ ::Kiq::JOBS.size }} } job classes"
      {% end %}

      launcher = Kiq::Launcher.new(config, config.expanded_queues,
        concurrency: config.concurrency, strict: false)

      done = Channel(Nil).new
      Signal::INT.trap { done.close }
      Signal::TERM.trap { done.close }
      Signal::TSTP.trap do
        puts "Kiq quiet: no new jobs will be started"
        launcher.quiet
      end

      launcher.run
      done.receive?
      puts "Kiq shutting down (waiting up to #{config.timeout}s for running jobs)..."
      launcher.stop
      puts "Kiq done"
    end

    private def banner(config) : String
      qs = @queues.map_with_index do |q, i|
        w = @weights[i]? || 1
        w > 0 ? "#{q},#{w}" : q
      end.join(" ")
      "Kiq #{Kiq::VERSION} (Sidekiq port) | concurrency #{config.concurrency} | queues #{qs} | redis #{config.redis_url}"
    end

    private def parse(argv) : Nil
      parser = OptionParser.new do |op|
        op.banner = "Usage: kiq [options]"
        op.on("-c INT", "--concurrency INT", "Processor fibers (default 5)") { |v| @concurrency = v.to_i }
        op.on("-q NAME[,WEIGHT]", "--queue NAME[,WEIGHT]", "Queues to poll, with optional weights (repeatable, default default,1)") do |v|
          parts = v.split(",", 2)
          @queues << parts[0]
          w = parts[1]?
          @weights << (w ? w.to_i : 1)
        end
        op.on("-t INT", "--timeout INT", "Graceful shutdown timeout in seconds (default 25)") { |v| @timeout = v.to_i }
        op.on("-v", "--verbose", "Debug logging") { @verbose = true }
        op.on("-V", "--version", "Print version and exit") do
          puts "Kiq #{Kiq::VERSION}"
          exit 0
        end
        op.on("-r PATH", "--require PATH", "NOT SUPPORTED at runtime (compile-time language): rebuild with KIQ_REQUIRE=PATH") do |v|
          @ignored_requires << v
        end
        op.on("-h", "--help", "Show this help") do
          puts op
          exit 0
        end
        op.invalid_option { |flag| STDERR.puts "Unknown flag #{flag}"; puts op; exit 1 }
      end
      parser.parse(argv)
      @ignored_requires.each do |path|
        STDERR.puts "WARNING: -r #{path} ignored — bake job classes at build time: KIQ_REQUIRE=#{path} shards build kiq"
      end
    end
  end
end

Kiq::CLI.new.run

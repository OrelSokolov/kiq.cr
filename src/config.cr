# Kiq::Config — 1:1 with Sidekiq::Config defaults (sidekiq 8.1.7).

module Kiq
  class Config
    property redis_url : String = ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0")
    property concurrency : Int32 = 5
    property timeout : Int32 = 25
    property poll_interval_average : Float64? = nil
    property average_scheduled_poll_interval : Float64 = 5
    property max_retries : Int32 = 25
    property dead_max_jobs : Int32 = 10_000
    property dead_timeout_in_seconds : Int32 = 180 * 24 * 60 * 60 # 6 months
    property logger : ::Log = ::Log.for("Kiq", ::Log::Severity::Error)

    property error_handlers : Array(Proc(Exception, Msg?, Nil)) = [] of Proc(Exception, Msg?, Nil)
    property death_handlers : Array(Proc(Msg, Exception, Nil)) = [] of Proc(Msg, Exception, Nil)

    property queues : Array(String) = ["default"]
    property queue_weights : Array(Int32)? = nil
    property environment : String = "development"

    @redis : ::Redis::Client?
    @client_chain : Middleware::Chain?
    @server_chain : Middleware::Chain?

    STATS_TTL = 5 * 365 * 24 * 60 * 60 # 5 years, 1:1

    def redis=(url : String)
      @redis = nil # rebuild the client lazily
      @redis_url = url
    end

    def redis_client : ::Redis::Client
      @redis ||= ::Redis::Client.new(URI.parse(redis_url))
    end

    def redis(&block : ::Redis::Client -> _) : _
      yield redis_client
    end

    def client_middleware : Middleware::Chain
      @client_chain ||= Middleware::Chain.new
    end

    def client_middleware(&block : Middleware::Chain -> Nil) : Middleware::Chain
      chain = client_middleware
      yield chain
      chain
    end

    def server_middleware : Middleware::Chain
      @server_chain ||= Middleware::Chain.new
    end

    def server_middleware(&block : Middleware::Chain -> Nil) : Middleware::Chain
      chain = server_middleware
      yield chain
      chain
    end

    def default_job_options : Msg
      {
        "queue" => JSON::Any.new("default"),
        "retry" => JSON::Any.new(true),
      }
    end

    # Queue mode: :strict (weights all zero), :random (all one) or
    # :weighted — same classification as Sidekiq::Capsule#queues=.
    def mode : Symbol
      weights = queue_weights
      return :strict unless weights
      return :strict if weights.all?(&.zero?)
      return :random if weights.all? { |w| w == 1 }
      :weighted
    end

    # Expand weighted queues into the BRPOP key list (1:1 with
    # Sidekiq::Capsule: `["critical", ["low", 2]]` → critical,low,low).
    def expanded_queues : Array(String)
      weights = queue_weights
      return queues if weights.nil?
      out = [] of String
      queues.each_with_index do |q, i|
        {weights[i]? || 0, 1}.max.times { out << q }
      end
      out
    end
  end
end

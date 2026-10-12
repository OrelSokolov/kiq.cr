# Kiq — 1:1 port of Sidekiq 8.1.7 for CRails.
#
# Namespace mapping: `Sidekiq` → `Kiq`, `sidekiq_options` → `kiq_options`.
# Wire format is byte-compatible with the original: jobs pushed by Kiq
# are visible to a real Sidekiq server and vice versa (verified by the
# oracle suite). See PLAN.md for documented deviations.

require "json"
require "redis"
require "random/secure"

require "./config"
require "./middleware"
require "./client"
require "./job"
require "./processor"
require "./scheduled"
require "./launcher"
require "./api"
require "./web_api"
require "./web"
require "./testing"

module Kiq
  VERSION = "8.1.7" # of the sidekiq release this port tracks

  NAME = "Kiq"

  # Job payload hash, 1:1 with sidekiq's msg (JSON round-tripped).
  alias Msg = Hash(String, JSON::Any)

  # JSON-native job argument values (sidekiq's "native JSON types"
  # best practice, enforced by verify_json in the original).
  alias Arg = Nil | Bool | Int32 | Int64 | Float64 | String | Array(Arg) | Hash(String, Arg)

  # Compile-time registry of all classes that `include Kiq::Job`.
  JOBS = [] of Nil

  # Compile-time per-class kiq_options (class name => NamedTupleLiteral).
  OPTIONS = {} of Nil => Nil

  # Deferred `configure_server` blocks (run by the Launcher, exactly what
  # the sidekiq CLI does at boot; outside a server process the block is
  # stored, not executed — same standalone semantics as sidekiq 8).
  SERVER_BLOCKS = [] of Proc(Config, Nil)

  class_property? server : Bool = false

  def self.default_configuration : Config
    @@default_configuration ||= Config.new
  end

  def self.configure_server(&block : Config -> Nil) : Nil
    SERVER_BLOCKS << block
    block.call(default_configuration) if server?
  end

  def self.configure_client(&block : Config -> Nil) : Nil
    block.call(default_configuration)
  end

  def self.configure(&block : Config -> Nil) : Nil
    block.call(default_configuration)
  end

  def self.redis(&block : ::Redis::Client -> _) : _
    yield default_configuration.redis_client
  end

  def self.load_json(string : String) : Msg
    JSON.parse(string).as_h
  end

  def self.dump_json(msg : Msg) : String
    string = String.build do |io|
      JSON.build(io) do |builder|
        write_msg(builder, msg)
      end
    end
    string
  end

  def self.write_msg(io : JSON::Builder, msg : Msg) : Nil
    io.object do
      msg.each do |k, v|
        io.field(k) { v.to_json(io) }
      end
    end
  end

  def self.now_ms : Int64
    Time.utc.to_unix_ms
  end

  def self.new_jid : String
    Random::Secure.hex(12)
  end

  def self.handle_exception(ex : Exception, ctx : Msg? = nil) : Nil
    default_configuration.error_handlers.each do |handler|
      begin
        handler.call(ex, ctx)
      rescue ex2 : Exception
        # best effort, like sidekiq
      end
    end
  end

  def self.testing!(mode : String, &block : -> _)
    case mode
    when "fake", "inline", "disable"
      Testing.__set_test_mode(mode, &block)
    else
      raise ArgumentError.new("Unknown testing mode: #{mode}")
    end
  end
end

# Batch/DAG layer (sidekiq-dags port). Required after the module body so
# that Kiq::Job's compile-time registries exist when node/callback classes
# register themselves.
require "./batch"
require "./node"

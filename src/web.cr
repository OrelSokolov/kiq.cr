# Kiq::Web — 1:1 port of Sidekiq::Web (sidekiq 8.1.7): router, actions,
# helpers, cookie session, CSP nonces, locales and embedded static assets.
# Rendered as an HTTP::Handler (the Rack app becomes a Crystal handler);
# the templates under web/views are translated from the original ERB.
#
# Documented deviations (see PLAN.md): Metrics are counts-only (no
# execution-time histograms), WorkSet stays empty (no per-job work
# tracking), product name is "Kiq" everywhere the original shows "Sidekiq".

require "ecr"
require "http/server"
require "openssl"
require "openssl/hmac"

module Kiq
  class Web
    include HTTP::Handler

    REDIS_KEYS = ["redis_version", "uptime_in_days", "connected_clients", "used_memory_human", "used_memory_peak_human"]

    CSP_HEADER_TEMPLATE = [
      "default-src 'self' https: http:",
      "child-src 'self'",
      "connect-src 'self' https: http: wss: ws:",
      "font-src 'none'",
      "frame-src 'self'",
      "img-src 'self' https: http: data:",
      "manifest-src 'self'",
      "media-src 'self'",
      "object-src 'none'",
      "script-src 'self' 'nonce-!placeholder!'",
      "style-src 'self' 'nonce-!placeholder!'",
      "worker-src 'self'",
      "base-uri 'self'",
    ].join("; ")

    # period code => window in minutes (sidekiq METRICS_PERIODS)
    METRICS_PERIODS = {"1h" => 60, "2h" => 120, "4h" => 240, "8h" => 480, "24h" => 1440, "48h" => 2880, "72h" => 4320}

    TABS = [{"Dashboard", ""}, {"Busy", "busy"}, {"Queues", "queues"}, {"Retries", "retries"},
            {"Scheduled", "scheduled"}, {"Dead", "morgue"}, {"Metrics", "metrics"}, {"Profiles", "profiles"}] of {String, String}

    struct Asset
      getter mime : String
      getter body : String

      def initialize(@mime, @body)
      end
    end

    ASSETS = {
      "/stylesheets/style.css"              => Asset.new("text/css", {{ read_file "#{__DIR__}/web/assets/stylesheets/style.css" }}),
      "/javascripts/application.js"         => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/application.js" }}),
      "/javascripts/base-charts.js"         => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/base-charts.js" }}),
      "/javascripts/chart.min.js"           => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/chart.min.js" }}),
      "/javascripts/chartjs-adapter-date-fns.min.js" => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/chartjs-adapter-date-fns.min.js" }}),
      "/javascripts/chartjs-plugin-annotation.min.js" => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/chartjs-plugin-annotation.min.js" }}),
      "/javascripts/dashboard-charts.js"    => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/dashboard-charts.js" }}),
      "/javascripts/dashboard.js"           => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/dashboard.js" }}),
      "/javascripts/metrics.js"             => Asset.new("text/javascript", {{ read_file "#{__DIR__}/web/assets/javascripts/metrics.js" }}),
      "/images/apple-touch-icon.png"        => Asset.new("image/png", {{ read_file "#{__DIR__}/web/assets/images/apple-touch-icon.png" }}),
      "/images/favicon.ico"                 => Asset.new("image/x-icon", {{ read_file "#{__DIR__}/web/assets/images/favicon.ico" }}),
      "/images/logo.png"                    => Asset.new("image/png", {{ read_file "#{__DIR__}/web/assets/images/logo.png" }}),
      "/images/status.png"                  => Asset.new("image/png", {{ read_file "#{__DIR__}/web/assets/images/status.png" }}),
    }

    LOCALE_SOURCES = {
      "en" => {{ read_file "#{__DIR__}/web/locales/en.yml" }},
      "ru" => {{ read_file "#{__DIR__}/web/locales/ru.yml" }},
    }

    # Rack::Session::Cookie equivalent: HMAC-signed JSON cookie.
    class_property session_secret : String = Random::Secure.hex(32)

    # Adds the "Back to App" link in the header.
    class_property app_url : String? = nil

    @@strings = {} of String => Hash(String, String)

    def self.strings_for(lang : String) : Hash(String, String)
      @@strings[lang] ||= parse_locale(LOCALE_SOURCES[lang]? || "", lang)
    end

    def self.available_locales : Array(String)
      LOCALE_SOURCES.keys
    end

    # Sidekiq's WebHelpers#parse_yaml — a deliberately tiny line-based YAML
    # subset parser for the locale files.
    def self.parse_locale(src : String, lang : String) : Hash(String, String)
      map = {} of String => String
      current_lang : String? = nil
      src.each_line do |line|
        line = line.chomp
        if line =~ /\A\s*\#.*/
          # line comment
        elsif current_lang.nil? && (match = line.match(/\A([a-zA-Z\-_]+):/))
          current_lang = match[1]
        elsif (match = line.match(/\A\s+(\w+):\s+(.+)\z/))
          if current_lang == lang
            key = match[1]
            s = match[2]
            s = s[1..] if s[0]? == '"'
            s = s[0..-2] if s[-1]? == '"'
            map[key] = s
          end
        else
          raise ArgumentError.new("unable to parse locale #{lang}: #{line}")
        end
      end
      map
    end

    def self.redis_info : Hash(String, String)
      Kiq.default_configuration.redis_client.info
    end

    def self.store_name : String
      hash = redis_info
      return "Dragonfly" if hash.has_key?("dragonfly_version")
      return "Valkey" if hash.has_key?("valkey_version")
      "Redis"
    end

    def self.store_version : String
      hash = redis_info
      return hash["dragonfly_version"] if hash.has_key?("dragonfly_version")
      return hash["valkey_version"] if hash.has_key?("valkey_version")
      hash["redis_version"]
    end

    # CGI.escape equivalent (form encoding: space -> "+").
    def self.escape(s : String) : String
      URI.encode_www_form(s)
    end

    class HaltSignal < Exception
      getter status : Int32
      getter location : String?
      getter body : String?
      getter headers : Hash(String, String)
      getter? external : Bool

      def initialize(@status, @location = nil, @body = nil, @headers = {} of String => String, @external = false)
        super("")
      end
    end

    struct Route
      getter method : String
      getter segments : Array(String)
      getter block : Proc(Action, String)

      def initialize(@method, pattern : String, &block : Action -> String)
        @segments = pattern.split('/').reject(&.empty?)
        @block = block
      end

      def match?(path : String) : Hash(String, String)?
        parts = path.split('/').reject(&.empty?)
        return nil unless parts.size == @segments.size
        params = {} of String => String
        @segments.each_with_index do |seg, i|
          if seg.starts_with?(':')
            params[seg[1..]] = parts[i]
          else
            return nil unless seg == parts[i]
          end
        end
        params
      end
    end

    @@routes : Array(Route)?

    def self.routes : Array(Route)
      @@routes ||= [
        Route.new("HEAD", "/") { |a| a.root_heartbeat },
        Route.new("GET", "/") { |a| a.dashboard },
        Route.new("GET", "/metrics") { |a| a.metrics_index },
        Route.new("GET", "/metrics/:name") { |a| a.metrics_show },
        Route.new("GET", "/busy") { |a| a.busy_index },
        Route.new("POST", "/busy") { |a| a.busy_create },
        Route.new("GET", "/queues") { |a| a.queues_index },
        Route.new("GET", "/queues/:name") { |a| a.queue_show },
        Route.new("POST", "/queues/:name") { |a| a.queue_clear },
        Route.new("POST", "/queues/:name/delete") { |a| a.queue_delete_job },
        Route.new("GET", "/morgue") { |a| a.morgue_index },
        Route.new("GET", "/morgue/:key") { |a| a.morgue_show },
        Route.new("POST", "/morgue") { |a| a.morgue_bulk },
        Route.new("POST", "/morgue/all/delete") { |a| a.morgue_delete_all },
        Route.new("POST", "/morgue/all/retry") { |a| a.morgue_retry_all },
        Route.new("POST", "/morgue/:key") { |a| a.morgue_update },
        Route.new("GET", "/retries") { |a| a.retries_index },
        Route.new("GET", "/retries/:key") { |a| a.retries_show },
        Route.new("POST", "/retries") { |a| a.retries_bulk },
        Route.new("POST", "/retries/all/delete") { |a| a.retries_delete_all },
        Route.new("POST", "/retries/all/retry") { |a| a.retries_retry_all },
        Route.new("POST", "/retries/all/kill") { |a| a.retries_kill_all },
        Route.new("POST", "/retries/:key") { |a| a.retries_update },
        Route.new("GET", "/scheduled") { |a| a.scheduled_index },
        Route.new("GET", "/scheduled/:key") { |a| a.scheduled_show },
        Route.new("POST", "/scheduled") { |a| a.scheduled_bulk },
        Route.new("POST", "/scheduled/:key") { |a| a.scheduled_update },
        Route.new("POST", "/scheduled/all/delete") { |a| a.scheduled_delete_all },
        Route.new("POST", "/scheduled/all/add_to_queue") { |a| a.scheduled_enqueue_all },
        Route.new("GET", "/dashboard/stats") { |a| a.dashboard_stats_redirect },
        Route.new("GET", "/stats") { |a| a.stats_index },
        Route.new("GET", "/stats/queues") { |a| a.stats_queues },
        Route.new("GET", "/profiles") { |a| a.profiles_index },
        Route.new("GET", "/profiles/:key") { |a| a.profile_show },
        Route.new("GET", "/profiles/:key/data") { |a| a.profile_data },
        Route.new("POST", "/change_locale") { |a| a.change_locale },
      ]
    end

    def call(context : HTTP::Server::Context)
      request = context.request
      unless safe_request?(request)
        resp = context.response
        resp.status_code = 403
        resp.headers["content-type"] = "text/plain"
        resp.print("Forbidden")
        return
      end

      path = Web.percent_decode(request.path.presence || "/")
      path = "/" if path.empty?

      if request.method.in?({"GET", "HEAD"}) && (asset = ASSETS[path]?)
        serve_asset(context, asset)
        return
      end

      matched = nil
      params = nil
      Web.routes.each do |candidate|
        if candidate.method == request.method && (m = candidate.match?(path))
          matched = candidate
          params = m
          break
        end
      end

      if matched && (route = matched.as(Route)) && (rp = params)
        action = Action.new(context, rp, Random::Secure.hex(8), path)
        action.run(route.block)
      else
        resp = context.response
        resp.status_code = 404
        resp.headers["content-type"] = "text/plain"
        resp.headers["x-cascade"] = "pass"
        resp.print("Not Found")
      end
    end

    private def serve_asset(context, asset : Asset)
      resp = context.response
      resp.status_code = 200
      resp.headers["content-type"] = asset.mime
      resp.headers["cache-control"] = "private, max-age=86400" unless ENV["SIDEKIQ_WEB_TESTING"]?
      resp.print(asset.body)
    end

    private def safe_request?(request) : Bool
      return true if request.method.in?({"GET", "HEAD", "OPTIONS", "TRACE"})
      request.headers["Sec-Fetch-Site"]? == "same-origin"
    end

    def self.percent_decode(s : String) : String
      return s unless s.includes?('%')
      bytes = s.to_slice
      buf = IO::Memory.new(bytes.size)
      i = 0
      while i < bytes.size
        b = bytes[i]
        if b == 0x25 && i + 2 < bytes.size # '%'
          hi = hex_val(bytes[i + 1])
          lo = hex_val(bytes[i + 2])
          if hi && lo
            buf.write_byte(((hi << 4) | lo).to_u8!)
            i += 3
            next
          end
        end
        buf.write_byte(b)
        i += 1
      end
      String.new(buf.to_slice)
    end

    private def self.hex_val(b : UInt8) : Int32?
      case b
      when 0x30..0x39 then (b - 0x30).to_i
      when 0x41..0x46 then (b - 0x41 + 10).to_i
      when 0x61..0x66 then (b - 0x61 + 10).to_i
      else nil
      end
    end
  end
end

require "./web/action"
require "./web/metrics"
require "./web/views/views"

# Kiq::Web::Action — port of sidekiq's Web::Action + WebHelpers +
# Paginator: one instance per request, runs a route block and translates
# HaltSignal (redirect/halt/json) into an HTTP response.

module Kiq
  class Web
    # Per-request context handed to the views (the subset of the original
    # Action/env surface the ECR templates rely on).
    class Ctx
      getter nonce : String
      getter path : String
      getter params : Hash(String, Array(String))
      getter session : Hash(String, String)
      getter locale : String

      def initialize(@nonce, @path, @params, @session, accept_language : String?)
        @locale = determine_locale(@session["locale"]?, accept_language)
      end

      def root_path : String
        "/"
      end

      def current_path : String
        @path.sub(/\A\//, "")
      end

      def url_params(key : String) : String?
        @params[key]?.try(&.first?)
      end

      private def determine_locale(session_locale : String?, accept_language : String?) : String
        if session_locale && Web.available_locales.includes?(session_locale)
          return session_locale
        end
        match_accept_language(accept_language) || "en"
      end

      # RFC 9110 Accept-Language matching, 1:1 with WebHelpers#locale.
      private def match_accept_language(header : String?) : String?
        return nil if header.nil? || header.empty?
        langs = header.not_nil!.gsub(/\s+/, "").split(",").map do |l|
          parts = l.split(";q=", 2)
          locale = parts[0]?
          quality = parts[1]?.try(&.to_f64?) || 1.0
          {locale.nil? || locale == "*" ? nil : locale, quality}
        end.sort { |a, b| b[1] <=> a[1] }.map(&.[0]).compact

        langs.each do |preferred|
          # "en-GB-oxendict" becomes "en-GB"
          tag = preferred.split("-")[0..1].join("-")
          m = Web.available_locales.find { |l| l.downcase == tag.downcase }
          return m if m
        end
        # First base language match: "en-US,es-MX;q=0.9" matches "en"
        langs.each do |preferred|
          base = preferred.split("-", 2).first
          m = Web.available_locales.find { |l| l.downcase == base.downcase }
          return m if m
        end
        nil
      end
    end

    class Action
      SESSION_COOKIE = "kiq.session"

      getter ctx : Ctx
      getter context : HTTP::Server::Context
      getter route_params : Hash(String, String)
      getter nonce : String

      @response_headers = {} of String => String
      @session_dirty = false
      @session : Hash(String, String)

      def initialize(@context, @route_params, @nonce, decoded_path : String)
        request = @context.request
        params = build_params(request)
        session = load_session(request)
        @ctx = Ctx.new(@nonce, decoded_path, params, session, request.headers["Accept-Language"]?)
        @session = session
      end

      def run(block : Proc(Action, String)) : Nil
        resp = @context.response
        begin
          body = block.call(self)
          resp.status_code = 200
          resp.headers["content-type"] = "text/html"
          resp.headers["cache-control"] = "private, no-store"
          resp.headers["content-language"] = @ctx.locale
          resp.headers["content-security-policy"] = Web::CSP_HEADER_TEMPLATE.gsub("!placeholder!", @nonce)
          resp.headers["x-content-type-options"] = "nosniff"
          @response_headers.each { |k, v| resp.headers[k] = v }
          save_session(resp)
          resp.print(body)
        rescue ex : HaltSignal
          if (loc = ex.location)
            resp.status_code = 302
            resp.headers["location"] = ex.external? ? loc : base_url + loc
          else
            resp.status_code = ex.status
            resp.headers["content-type"] = "text/plain"
            ex.headers.each { |k, v| resp.headers[k] = v }
          end
          save_session(resp)
          resp.print(ex.body || ex.status.to_s)
        end
      end

      # --- request plumbing (Web::Action) -------------------------------------

      private def build_params(request) : Hash(String, Array(String))
        params = {} of String => Array(String)
        append = ->(params : Hash(String, Array(String)), source : HTTP::Params) do
          # Rack-style params: a "key[]" suffix collects values under "key"
          source.each do |k, v|
            k = k.rchop("[]")
            (params[k] ||= [] of String) << v
          end
        end
        append.call(params, HTTP::Params.parse(request.query || ""))
        if request.method == "POST"
          ct = request.headers["content-type"]?
          if ct && ct.starts_with?("application/x-www-form-urlencoded")
            body = request.body.try(&.gets_to_end) || ""
            append.call(params, HTTP::Params.parse(body))
          end
        end
        params
      end

      private def load_session(request) : Hash(String, String)
        raw = request.cookies[SESSION_COOKIE]?.try(&.value)
        return {} of String => String unless raw
        payload, sig = raw.split("--", 2)
        expected = OpenSSL::HMAC.hexdigest(OpenSSL::Algorithm::SHA256, Web.session_secret, payload)
        return {} of String => String unless sig == expected
        JSON.parse(Base64.decode_string(payload)).as_h.each_with_object({} of String => String) do |(k, v), h|
          h[k] = v.to_s
        end
      rescue
        {} of String => String
      end

      private def save_session(resp) : Nil
        return unless @session_dirty
        payload = Base64.urlsafe_encode(JSON.build do |json|
          json.object do
            @session.each { |k, v| json.field k, v }
          end
        end)
        sig = OpenSSL::HMAC.hexdigest(OpenSSL::Algorithm::SHA256, Web.session_secret, payload)
        resp.cookies << HTTP::Cookie.new(SESSION_COOKIE, "#{payload}--#{sig}", path: "/")
      end

      def request : HTTP::Request
        @context.request
      end

      def base_url : String
        "http://" + (@context.request.headers["Host"]? || "localhost")
      end

      def halt(status : Int32, msg : String? = nil) : NoReturn
        raise HaltSignal.new(status, body: msg)
      end

      def redirect(location : String) : NoReturn
        raise HaltSignal.new(302, location: location)
      end

      # external redirect (no base_url prefix)
      def redirect_to(url : String) : NoReturn
        raise HaltSignal.new(302, location: url, external: true)
      end

      def header(key : String, value : String) : Nil
        @response_headers[key] = value
      end

      def json(payload : String) : NoReturn
        raise HaltSignal.new(200, body: payload,
          headers: {"content-type" => "application/json", "cache-control" => "private, no-store"})
      end

      def reload_page : NoReturn
        current_location = (request.headers["Referer"]? || "").gsub(base_url, "")
        redirect current_location
      end

      # Any paginated list that performs an action needs to redirect
      # back to the proper page after performing that action.
      def redirect_with_query(url : String) : NoReturn
        r = request.headers["Referer"]?
        if r && r.includes?("?")
          redirect("#{url}?#{r.split("?", 2)[1]}")
        else
          redirect url
        end
      end

      def url_params(key : String) : String?
        @ctx.url_params(key)
      end

      # --- Paginator (sidekiq/paginator.rb) ------------------------------------

      # Ruby String#to_i semantics: leading integer or 0.
      private def to_i(s : String?) : Int32
        s.try(&.to_i?) || 0
      end

      # Paginate a queue LIST (sidekiq Paginator#page, type "list").
      def page_list(key : String, pageidx : String?, page_size : Int32, reverse : Bool = false) : {Int32, Int64, Array(String)}
        current_page = to_i(pageidx)
        current_page = 1 if current_page < 1
        starting = (current_page - 1) * page_size
        ending = starting + page_size - 1
        redis = Kiq.default_configuration.redis_client
        total_size = redis.llen(key)
        items = if reverse
                  arr = redis.lrange(key, (-ending - 1).to_i64, (-starting - 1).to_i64)
                  arr.map(&.to_s).reverse
                else
                  redis.lrange(key, starting.to_i64, ending.to_i64).map(&.to_s)
                end
        {current_page, total_size, items}
      end

      # Search helper (WebHelpers#search).
      def search(jobset : JobSet, substr : String) : Array(SortedEntry)
        resultset = jobset.scan(substr)
        resultset
      end

      # "score-jid" -> {score, jid}; garbage scores read as 0.0 (Ruby to_f).
      def parse_key(key : String) : {Float64, String}
        score, _, jid = key.partition("-")
        {score.to_f64? || 0.0, jid}
      end

      def retry_or_delete_or_kill(job : SortedEntry) : Nil
        job.retry if @ctx.params["retry"]?
        job.delete if @ctx.params["delete"]?
        job.kill if @ctx.params["kill"]?
      end

      def delete_or_add_queue(job : SortedEntry) : Nil
        job.delete if @ctx.params["delete"]?
        job.add_to_queue if @ctx.params["add_to_queue"]?
      end

      # --- route handlers (sidekiq/web/application.rb, same order) -------------

      def root_heartbeat : String
        _ = Kiq.default_configuration.redis_client.llen("queue:default")
        ""
      end

      def dashboard : String
        days = to_i(url_params("days") || "30")
        halt(401) if days < 1 || days > 180

        view = Views::Dashboard.new(@ctx)
        info = Web.redis_info
        Web::REDIS_KEYS.each { |k| view.redis_info[k] = info[k]? || "" }
        history = Stats::History.new(days)
        view.processed_history = history.processed
        view.failed_history = history.failed
        view.render
      end

      def metrics_index : String
        view = Views::Metrics.new(@ctx)
        view.period = h(url_params("period") || "1h")
        header("refresh", "60") if view.period == "1h"
        view.render
      end

      def metrics_show : String
        view = Views::MetricsForJob.new(@ctx)
        view.name = route_params["name"]
        view.period = h(url_params("period") || "1h")
        # Periods larger than 8 hours are not supported for histogram chart
        view.period = "8h" if to_i(view.period.rchop("h")) > 8
        header("refresh", "60") if view.period == "1h"
        view.render
      end

      def busy_index : String
        view = Views::Busy.new(@ctx)
        view.count = to_i(url_params("count") || "100")

        only = url_params("only")
        halt(401, "Invalid value for only") unless only.nil? || only == "jobs" || only == "processes"

        view.show_jobs = (only != "processes")
        view.show_processes = (only != "jobs")
        view.render
      end

      def busy_create : String
        if (identity = url_params("identity"))
          pro = ProcessSet.[](identity)

          pro.try(&.quiet!) if url_params("quiet")
          pro.try(&.stop!) if url_params("stop")
        else
          ProcessSet.new.each do |proc|
            next if proc.embedded?

            proc.quiet! if url_params("quiet")
            proc.stop! if url_params("stop")
          end
        end

        redirect "/busy"
      end

      def queues_index : String
        view = Views::Queues.new(@ctx)
        view.queues = Queue.all
        view.render
      end

      QUEUE_NAME = /\A[a-z_:.\-0-9]+\z/i

      def queue_show : String
        name = route_params["name"]
        halt(404) if !name || !(name =~ QUEUE_NAME)

        view = Views::QueuePage.new(@ctx)
        view.name = name
        view.count = to_i(url_params("count") || "25")
        current_page, total_size, msgs = page_list("queue:#{name}", url_params("page"), view.count,
          reverse: url_params("direction") == "asc")
        view.current_page = current_page
        view.total_size = total_size
        view.jobs = msgs.map { |msg| JobRecord.new(msg, name) }
        view.render
      end

      def queue_clear : String
        Queue.new(route_params["name"]).clear
        redirect "/queues"
      end

      def queue_delete_job : String
        name = route_params["name"]
        JobRecord.new(url_params("key_val").to_s, name).delete
        redirect_with_query("/queues/#{Web.escape(name)}")
      end

      def morgue_index : String
        view = Views::Morgue.new(@ctx)
        x = url_params("substr")

        if x && !x.empty?
          view.entries = search(DeadSet.new, x)
          view.current_page = 1
          view.total_size = view.entries.size.to_i64
          view.count = view.entries.size
        else
          view.count = to_i(url_params("count") || "25")
          current_page, total_size, entries = DeadSet.new.page(url_params("page"), view.count, reverse: true)
          view.current_page = current_page
          view.total_size = total_size
          view.entries = entries
        end
        view.render
      end

      def morgue_show : String
        key = route_params["key"]?
        halt(404) unless key

        score, jid = parse_key(key)
        job = DeadSet.new.fetch(score, jid).first?

        if job.nil?
          redirect "/morgue"
        else
          Views::Dead.new(@ctx, job).render
        end
      end

      def morgue_bulk : String
        keys = @ctx.params["key"]?
        if keys
          keys.each do |key|
            score, jid = parse_key(key)
            job = DeadSet.new.fetch(score, jid).first?
            retry_or_delete_or_kill(job) if job
          end

          redirect_with_query "/morgue"
        else
          redirect request.path
        end
      end

      def morgue_delete_all : String
        DeadSet.new.clear
        redirect "/morgue"
      end

      def morgue_retry_all : String
        DeadSet.new.retry_all
        redirect "/morgue"
      end

      def morgue_update : String
        key = route_params["key"]?
        halt(404) unless key

        score, jid = parse_key(key)
        job = DeadSet.new.fetch(score, jid).first?
        retry_or_delete_or_kill(job) if job

        redirect_with_query "/morgue"
      end

      def retries_index : String
        view = Views::Retries.new(@ctx)
        x = url_params("substr")

        if x && !x.empty?
          view.entries = search(RetrySet.new, x)
          view.current_page = 1
          view.total_size = view.entries.size.to_i64
          view.count = view.entries.size
        else
          view.count = to_i(url_params("count") || "25")
          current_page, total_size, entries = RetrySet.new.page(url_params("page"), view.count)
          view.current_page = current_page
          view.total_size = total_size
          view.entries = entries
        end
        view.render
      end

      def retries_show : String
        score, jid = parse_key(route_params["key"])
        job = RetrySet.new.fetch(score, jid).first?

        if job.nil?
          redirect "/retries"
        else
          Views::Retry.new(@ctx, job).render
        end
      end

      def retries_bulk : String
        keys = @ctx.params["key"]?
        if keys
          keys.each do |key|
            score, jid = parse_key(key)
            job = RetrySet.new.fetch(score, jid).first?
            retry_or_delete_or_kill(job) if job
          end

          redirect_with_query "/retries"
        else
          redirect request.path
        end
      end

      def retries_delete_all : String
        RetrySet.new.clear
        redirect "/retries"
      end

      def retries_retry_all : String
        RetrySet.new.retry_all
        redirect "/retries"
      end

      def retries_kill_all : String
        RetrySet.new.kill_all
        redirect "/retries"
      end

      def retries_update : String
        score, jid = parse_key(route_params["key"])
        job = RetrySet.new.fetch(score, jid).first?
        retry_or_delete_or_kill(job) if job

        redirect_with_query "/retries"
      end

      def scheduled_index : String
        view = Views::Scheduled.new(@ctx)
        x = url_params("substr")

        if x && !x.empty?
          view.entries = search(ScheduledSet.new, x)
          view.current_page = 1
          view.total_size = view.entries.size.to_i64
          view.count = view.entries.size
        else
          view.count = to_i(url_params("count") || "25")
          current_page, total_size, entries = ScheduledSet.new.page(url_params("page"), view.count)
          view.current_page = current_page
          view.total_size = total_size
          view.entries = entries
        end
        view.render
      end

      def scheduled_show : String
        score, jid = parse_key(route_params["key"])
        job = ScheduledSet.new.fetch(score, jid).first?

        if job.nil?
          redirect "/scheduled"
        else
          Views::ScheduledJob.new(@ctx, job).render
        end
      end

      def scheduled_bulk : String
        keys = @ctx.params["key"]?
        if keys
          keys.each do |key|
            score, jid = parse_key(key)
            job = ScheduledSet.new.fetch(score, jid).first?
            delete_or_add_queue(job) if job
          end

          redirect_with_query "/scheduled"
        else
          redirect request.path
        end
      end

      def scheduled_update : String
        key = route_params["key"]?
        halt(404) unless key

        score, jid = parse_key(key)
        job = ScheduledSet.new.fetch(score, jid).first?
        delete_or_add_queue(job) if job

        redirect_with_query "/scheduled"
      end

      def scheduled_delete_all : String
        ScheduledSet.new.clear
        redirect "/scheduled"
      end

      def scheduled_enqueue_all : String
        ScheduledSet.new.each(&.add_to_queue)
        redirect "/scheduled"
      end

      def dashboard_stats_redirect : String
        redirect "/stats"
      end

      def stats_index : String
        sidekiq_stats = Stats.new
        info = Web.redis_info
        json(JSON.build do |j|
          j.object do
            j.field "sidekiq" do
              j.object do
                j.field "processed", sidekiq_stats.processed
                j.field "failed", sidekiq_stats.failed
                j.field "busy", sidekiq_stats.workers_size
                j.field "processes", sidekiq_stats.processes_size
                j.field "enqueued", sidekiq_stats.enqueued
                j.field "scheduled", sidekiq_stats.scheduled_size
                j.field "retries", sidekiq_stats.retry_size
                j.field "dead", sidekiq_stats.dead_size
                j.field "default_latency", sidekiq_stats.default_queue_latency
              end
            end
            j.field "redis" do
              j.object do
                Web::REDIS_KEYS.each do |k|
                  j.field k, info[k]? || ""
                end
                j.field "store_name", Web.store_name
                j.field "store_version", Web.store_version
              end
            end
            j.field "server_utc_time", Time.utc.to_s("%H:%M:%S UTC")
          end
        end)
      end

      def stats_queues : String
        json(JSON.build do |j|
          j.object do
            Stats.new.queues.each do |name, size|
              j.field name, size
            end
          end
        end)
      end

      def profiles_index : String
        Views::Profiles.new(@ctx).render
      end

      def profile_show : String
        # no profile store configured in Kiq
        redirect_to "/profiles"
      end

      def profile_data : String
        data = Kiq.default_configuration.redis_client.hget(route_params["key"], "data").try(&.to_s) || ""
        raise HaltSignal.new(200, body: data, headers: {
          "content-type"                 => "application/json",
          "content-encoding"             => "gzip",
          "access-control-allow-origin"  => "*",
        })
      end

      def change_locale : String
        locale = url_params("locale").to_s

        match = Web.available_locales.find { |available| available == locale }

        if match
          @session["locale"] = match
          @session_dirty = true
        end

        reload_page
      end

      private def h(text) : String
        HTML.escape(text.to_s)
      end
    end
  end
end

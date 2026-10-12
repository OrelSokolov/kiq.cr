# Kiq::Web views — Page base (WebHelpers port) + one class per template,
# ECR-rendered from src/web/views/*.ecr (minimized ports of sidekiq 8.1.7's
# web/views/*.html.erb).

module Kiq
  class Web
    class Page
      getter ctx : Ctx

      # pagination state (Paginator in the original)
      property current_page : Int32 = 1
      property total_size : Int64 = 0
      property count : Int32 = 25

      def initialize(@ctx)
      end

      # Render this page wrapped in the layout (Action#erb + _render).
      def render : String
        Views::Layout.new(ctx, self.to_s).to_s
      end

      # --- WebHelpers port ------------------------------------------------

      def t(key : String, options = {} of String => String) : String
        string = Web.strings_for(ctx.locale)[key]? || Web.strings_for("en")[key]? || key
        options.each { |k, v| string = string.gsub("%{#{k}}", v.to_s) }
        string
      end

      def h(text) : String
        HTML.escape(text.to_s)
      end

      def root_path : String
        ctx.root_path
      end

      def nonce : String
        ctx.nonce
      end

      def locale : String
        ctx.locale
      end

      def text_direction : String
        Web.strings_for(locale)["TextDirection"]? || "ltr"
      end

      def url_params(key : String) : String?
        ctx.url_params(key)
      end

      def current_path : String
        ctx.current_path
      end

      def product_name : String
        Kiq::NAME
      end

      def product_version : String
        "#{Kiq::NAME} v#{Kiq::VERSION}"
      end

      def environment_title_prefix : String
        environment = Kiq.default_configuration.environment
        environment == "production" ? "" : "[#{environment.upcase}] "
      end

      def server_utc_time : String
        Time.utc.to_s("%H:%M:%S UTC")
      end

      def redis_url : String
        "" # original: conn.config.server_url, normally nil
      end

      def language_name(loc : String) : String
        Web.strings_for(loc)["LanguageName"]? || loc
      end

      def available_locales : Array(String)
        Web.available_locales
      end

      def pollable? : Bool
        !(current_path.empty? || current_path.includes?("metrics"))
      end

      def csrf_tag : String
        ""
      end

      def stats : Stats
        @stats ||= Stats.new
      end

      def workset : WorkSet
        @workset ||= WorkSet.new
      end

      def processes : ProcessSet
        @processes ||= ProcessSet.new
      end

      def sorted_processes : Array(Process)
        processes.to_a.sort_by { |p| p["hostname"].to_s }
      end

      def leader_identity : String?
        processes.leader.try(&.identity)
      end

      def relative_time(time : Time) : String
        utc = time.to_utc
        stamp = utc.to_s("%Y-%m-%dT%H:%M:%SZ")
        text = utc.to_s("%Y-%m-%d %H:%M:%S UTC")
        %(<time class="ltr" dir="ltr" title="#{stamp}" datetime="#{stamp}">#{text}</time>)
      end

      def time_from_any(v : JSON::Any?) : Time?
        return nil unless v
        case raw = v.raw
        when Int64
          raw > 1_000_000_000_000 ? Time.unix_ms(raw) : Time.unix(raw)
        when Float64
          raw > 1_000_000_000_000.0 ? Time.unix_ms(raw.to_i64) : Time.unix(raw.to_i64)
        else
          nil
        end
      end

      def number_with_delimiter(number : Int64 | Int32 | Float64, precision : Int32 = 0) : String
        if precision == 0
          rounded = number.is_a?(Float64) ? number.round.to_i64 : number.to_i64
          %(<span data-nwp="#{precision}">#{rounded}</span>)
        else
          %(<span data-nwp="#{precision}">#{number.round(precision)}</span>)
        end
      end

      def format_memory(rss_kb : Int64) : String
        return "0" if rss_kb == 0
        if rss_kb < 100_000
          "#{number_with_delimiter(rss_kb)} KB"
        elsif rss_kb < 10_000_000
          "#{number_with_delimiter((rss_kb / 1024.0).to_i64)} MB"
        else
          "#{number_with_delimiter((rss_kb / (1024.0 * 1024.0)), precision: 1)} GB"
        end
      end

      def truncate(text : String, truncate_after_chars : Int32 = 2000) : String
        text.size > truncate_after_chars ? "#{text[0..truncate_after_chars]}..." : text
      end

      # Ruby #inspect of a JSON-ish value (to_display in the original).
      def to_display(v : JSON::Any) : String
        case raw = v.raw
        when Nil        then "nil"
        when Bool       then raw.to_s
        when Int64      then raw.to_s
        when Float64    then raw.to_s
        when String     then raw.inspect
        when Array(JSON::Any::Type)
          "[" + raw.map { |e| to_display(JSON::Any.new(e)) }.join(", ") + "]"
        when Hash(String, JSON::Any::Type)
          "{" + raw.map { |k, e| "#{k.inspect} => #{to_display(JSON::Any.new(e))}" }.join(", ") + "}"
        else
          raw.to_s
        end
      end

      def to_display(arg : Arg) : String
        case arg
        when Nil          then "nil"
        when Bool         then arg.to_s
        when Int32, Int64 then arg.to_s
        when Float64      then arg.to_s
        when String       then arg.inspect
        when Array(Arg)
          "[" + arg.map { |a| to_display(a) }.join(", ") + "]"
        when Hash(String, Arg)
          "{" + arg.map { |k, a| "#{k.inspect} => #{to_display(a)}" }.join(", ") + "}"
        else
          arg.to_s
        end
      end

      def display_args(args : Array(JSON::Any), truncate_after_chars : Int32? = 2000) : String
        args.map { |a| h(truncate(to_display(a), truncate_after_chars || 2000)) }.join(", ")
      end

      def display_args(args : Array(Arg), truncate_after_chars : Int32? = 2000) : String
        args.map { |a| h(truncate(to_display(a), truncate_after_chars || 2000)) }.join(", ")
      end

      def args_inspect(args : Array(Arg)) : String
        "[" + args.map { |a| to_display(a) }.join(", ") + "]"
      end

      def job_params(entry : SortedEntry) : String
        "#{entry.score}-#{entry.jid}"
      end

      def filter_link(str : String, within : String? = "retries") : String
        hstr = h(str)
        within.nil? ? hstr : %Q{<a href='#{root_path}#{within}?substr=#{hstr}'>#{hstr}</a>}
      end

      def display_tags(tags : Array(String), within : String? = "retries") : String
        tags.map { |tag|
          %Q{<span class='label label-info jobtag jobtag-#{h(tag)}'>#{filter_link(tag, within)}</span>}
        }.join(" ")
      end

      def script_tag(location : String) : String
        src = location.includes?("://") || location.starts_with?(root_path) ? location : root_path + location
        %(<script type="text/javascript" src="#{src}" nonce="#{nonce}"></script>)
      end

      def sort_direction_label : String
        url_params("direction") == "asc" ? "&uarr;" : "&darr;"
      end

      # SAFE_QPARAMS + qparams(page: N) — used by _paging links.
      def qparams(overrides : Hash(String, String)) : String
        merged = {} of String => String
        ctx.params.each { |k, vals| merged[k] = vals.first }
        overrides.each { |k, v| merged[k] = v }
        parts = [] of String
        {"page", "direction", "only"}.each do |safe|
          if (v = merged[safe]?)
            parts << "#{Web.escape(safe)}=#{Web.escape(v)}"
          end
        end
        parts.join("&")
      end

      # _paging.html.erb
      def paging(url : String) : String
        return "" unless total_size > count
        String.build do |s|
          s << %(<div>\n<ul class="pagination pull-right">)
          s << %(<li class="#{"disabled" if current_page == 1}"><a href="#{url}?page=1">&laquo;</a></li>)
          if current_page > 1
            s << %(<li><a href="#{url}?#{qparams({"page" => (current_page - 1).to_s})}">#{current_page - 1}</a></li>)
          end
          s << %(<li class="active"><a href="#{url}?#{qparams({"page" => current_page.to_s})}">#{current_page}</a></li>)
          if total_size > current_page * count
            s << %(<li><a href="#{url}?#{qparams({"page" => (current_page + 1).to_s})}">#{current_page + 1}</a></li>)
          end
          last = ((total_size.to_f / count).ceil).to_i
          s << %(<li class="#{"disabled" if total_size <= current_page * count}"><a href="#{url}?page=#{last}">&raquo;</a></li>)
          s << %(</ul>\n</div>)
        end
      end

      # filtering.html.erb
      def filtering(which : String, placeholder_key : String = "AnyJobContent", label_key : String = "Filter") : String
        String.build do |s|
          s << %(<div class="filter">\n)
          s << %(  <form role="search" method="get" class="form-inline" action="#{root_path}#{which}">\n)
          s << %(    <label for="substr">#{t(label_key)}</label>\n)
          s << %(    <input class="form-control" type="search" name="substr" value="#{h(url_params("substr").to_s)}" placeholder="#{t(placeholder_key)}" autocomplete="off">\n)
          s << %(  </form>\n</div>)
        end
      end

      # _poll_link.html.erb
      def poll_link : String
        return "" unless pollable?
        %(<a href="#" class="btn live-poll live-poll-start btn-primary">#{t("LivePoll")}</a>\n) +
          %(<a href="#" class="btn live-poll live-poll-stop btn-danger">#{t("StopPolling")}</a>)
      end

      # _metrics_period_select.html.erb
      def metrics_period_select(path : String) : String
        String.build do |s|
          s << %(<div class="filter">\n)
          s << %(  <form id="metrics-form" class="form-inline" action="#{path}" method="get">\n)
          s << %(    <label for="substr">#{t("Filter")}</label>\n)
          s << %(    <input id="class-filter" class="form-control" type="text" name="substr" placeholder="#{t("Name")}" value="#{h(url_params("substr").to_s)}" autocomplete="off">\n)
          s << %(    <select id="period-selector" class="form-control" name="period">\n)
          Web::METRICS_PERIODS.each do |code|
            if code == @period
              s << %(        <option selected value="#{code}">#{code}</option>\n)
            else
              s << %(        <option value="#{code}">#{code}</option>\n)
            end
          end
          s << %(    </select>\n  </form>\n</div>)
        end
      end

      # process view helpers (busy page)
      def process_version(process : Process) : String?
        process["version"]?.try(&.to_s)
      end

      def queue_names_by_capsule(process : Process) : String
        process.queues.join(", ")
      end
    end

    module Views
      class Layout < Page
        def initialize(ctx, @content : String)
          super(ctx)
        end

        def content : String
          @content
        end

        ECR.def_to_s("#{__DIR__}/layout.ecr")
      end

      class Nav < Page
        ECR.def_to_s("#{__DIR__}/nav.ecr")
      end

      class Summary < Page
        ECR.def_to_s("#{__DIR__}/summary.ecr")
      end

      class Footer < Page
        ECR.def_to_s("#{__DIR__}/footer.ecr")
      end

      class Dashboard < Page
        property redis_info : Hash(String, String) = {} of String => String
        property processed_history : Hash(String, Int64) = {} of String => Int64
        property failed_history : Hash(String, Int64) = {} of String => Int64

        ECR.def_to_s("#{__DIR__}/dashboard.ecr")

        def store_name : String
          Web.store_name
        end

        def store_version : String
          Web.store_version
        end

        def realtime_chart_json : String
          JSON.build do |json|
            json.object do
              json.field "processedLabel", t("Processed")
              json.field "failedLabel", t("Failed")
              json.field("labels") do
                json.array { 50.times { json.string "" } }
              end
              json.field("processed") do
                json.array { 50.times { json.null } }
              end
              json.field("failed") do
                json.array { 50.times { json.null } }
              end
              json.field "updateUrl", "#{root_path}stats"
            end
          end
        end

        def history_chart_json : String
          JSON.build do |json|
            json.object do
              json.field "processedLabel", t("Processed")
              json.field "failedLabel", t("Failed")
              json.field("processed") { history_pairs(json, processed_history) }
              json.field("failed") { history_pairs(json, failed_history) }
            end
          end
        end

        private def history_pairs(json, hash)
          json.array do
            hash.to_a.reverse.each do |k, v|
              json.array do
                json.string k
                json.scalar v
              end
            end
          end
        end
      end

      class Busy < Page
        property? show_processes : Bool = true
        property? show_jobs : Bool = true

        ECR.def_to_s("#{__DIR__}/busy.ecr")

        def utilization : Int64
          x = processes.total_concurrency
          ws = workset.size
          return 0_i64 if x == 0
          ((ws.to_f / x.to_f) * 100).round.to_i64
        end
      end

      class Queues < Page
        property queues : Array(Kiq::Queue) = [] of Kiq::Queue

        ECR.def_to_s("#{__DIR__}/queues.ecr")
      end

      class QueuePage < Page
        property name : String = ""
        property jobs : Array(Kiq::JobRecord) = [] of Kiq::JobRecord

        ECR.def_to_s("#{__DIR__}/queue.ecr")

        def queue_url : String
          "#{root_path}queues/#{Web.escape(name)}"
        end
      end

      class SetPage < Page
        property entries : Array(Kiq::SortedEntry) = [] of Kiq::SortedEntry
      end

      class Retries < SetPage
        ECR.def_to_s("#{__DIR__}/retries.ecr")
      end

      class Scheduled < SetPage
        ECR.def_to_s("#{__DIR__}/scheduled.ecr")
      end

      class Morgue < SetPage
        ECR.def_to_s("#{__DIR__}/morgue.ecr")
      end

      class JobInfo < Page
        getter job : SortedEntry
        getter type : Symbol

        def initialize(ctx, @job, @type)
          super(ctx)
        end

        RETRY_JOB_KEYS = %w[
          queue class args retry_count retried_at failed_at
          jid error_message error_class backtrace
          error_backtrace enqueued_at retry wrapped
          created_at tags display_class
        ]

        def retry_extra_items : Array(Tuple(String, JSON::Any))
          extra = [] of Tuple(String, JSON::Any)
          job.item.each do |k, v|
            extra << {k, v} unless RETRY_JOB_KEYS.includes?(k)
          end
          extra
        end

        def retry_count : Int64?
          job["retry_count"]?.try(&.to_s.to_i64?)
        end

        ECR.def_to_s("#{__DIR__}/job_info.ecr")
      end

      class Retry < Page
        getter job : SortedEntry

        def initialize(ctx, @job)
          super(ctx)
        end

        ECR.def_to_s("#{__DIR__}/retry.ecr")
      end

      class Dead < Page
        getter job : SortedEntry

        def initialize(ctx, @job)
          super(ctx)
        end

        ECR.def_to_s("#{__DIR__}/dead.ecr")
      end

      class ScheduledJob < Page
        getter job : SortedEntry

        def initialize(ctx, @job)
          super(ctx)
        end

        ECR.def_to_s("#{__DIR__}/scheduled_job.ecr")
      end

      class Metrics < Page
        property period : String = "1h"

        ECR.def_to_s("#{__DIR__}/metrics.ecr")
      end

      class MetricsForJob < Page
        property name : String = ""
        property period : String = "1h"

        ECR.def_to_s("#{__DIR__}/metrics_for_job.ecr")
      end

      class Profiles < Page
        ECR.def_to_s("#{__DIR__}/profiles.ecr")
      end
    end
  end
end

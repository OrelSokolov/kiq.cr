# Oracle cases for the Sidekiq Web UI port "Kiq::Web" (kiq.cr).
#
# Run against the ORIGINAL gem:
#   TZ=UTC ruby oracle/web_cases.rb oracle/expected_web.yml
#
# Same convention as oracle/cases.rb: "<case_id>\t<value>", mirrored 1:1 in
# spec/web_oracle_spec.cr. Cases drive the Web UI over HTTP (Rack::MockRequest
# here; the Crystal mirror serves Kiq::Web on a local port) and capture
# status codes, redirects and normalized structural markers of the HTML, plus
# Redis state after mutating actions.
#
# Requires: local Redis (DB 15 is flushed), sidekiq = 8.1.7 (with rack).

ENV["TZ"] = "UTC"

require "sidekiq"
require "sidekiq/api"
require "sidekiq/web"
require "rack"
require "rack/mock"
gem "rack-session", ">= 2.0"
require "rack/session" # rack-session gem provides Rack::Session::Cookie for rack 3.x
require "json"

raise "sidekiq 8.1.7 expected, got #{Sidekiq::VERSION}" unless Sidekiq::VERSION == "8.1.7"

URL = ENV.fetch("KIQ_ORACLE_REDIS", "redis://127.0.0.1:6379/15")

Sidekiq.configure_client do |config|
  config.redis = {url: URL}
  config.logger = Logger.new(File::NULL)
end
# The Web app uses Sidekiq.default_configuration.redis_pool
Sidekiq.default_configuration.redis = {url: URL}
Sidekiq.default_configuration.logger = Logger.new(File::NULL)

# deterministic far-future timestamps for ZSET scores (2030-01-01 etc.)
T1 = Time.utc(2030, 1, 1, 0, 0, 0).to_f
T2 = Time.utc(2030, 1, 2, 0, 0, 0).to_f
JID_A = "aaaaaaaaaaaaaaaaaaaaaaaa" # 24 hex chars, fixed
JID_B = "bbbbbbbbbbbbbbbbbbbbbbbb"
JID_C = "cccccccccccccccccccccccc"
JID_D = "dddddddddddddddddddddddd"
IDENT = "oraclehost:7001:0123456789abcdef"

OUT = []
def emit(id, value)
  OUT << [id, value.to_s].join("\t")
  warn "== #{id}: #{value}"
end

def redis
  Sidekiq.redis { |c| yield c }
end

# --- seed helpers ------------------------------------------------------------

def seed
  Sidekiq.redis { |c| c.flushdb }

  # queues
  Sidekiq::Client.push("class" => "WebA", "args" => [1, "x"], "jid" => JID_A, "created_at" => 1750000000.0, "enqueued_at" => 1750000000.0)
  Sidekiq::Client.push("class" => "WebB", "args" => ["hello <b>&amp;</b>"], "jid" => JID_B, "created_at" => 1750000000.0, "enqueued_at" => 1750000000.0)
  Sidekiq::Client.push("class" => "WebA", "args" => [[1, 2], {"k" => "v"}], "jid" => JID_C, "created_at" => 1750000000.0, "enqueued_at" => 1750000000.0)
  Sidekiq::Client.push("class" => "WebC", "queue" => "critical", "args" => [42], "jid" => JID_D, "created_at" => 1750000000.0, "enqueued_at" => 1750000000.0)

  # retry set: one WebA with error info, one WebB
  r1 = Sidekiq.load_json(Sidekiq.dump_json("class" => "WebA", "args" => [1], "queue" => "default", "jid" => JID_A,
    "error_message" => "kaboom", "error_class" => "RuntimeError", "retry_count" => 2,
    "failed_at" => 1750000000.0, "retried_at" => 1750000100.0, "created_at" => 1750000000.0))
  r2 = Sidekiq.load_json(Sidekiq.dump_json("class" => "WebB", "args" => ["y"], "queue" => "default", "jid" => JID_B,
    "error_message" => "other", "error_class" => "ArgumentError", "retry_count" => 0,
    "failed_at" => 1750000000.0, "created_at" => 1750000000.0))
  Sidekiq.redis do |c|
    c.zadd("retry", T1, Sidekiq.dump_json(r1))
    c.zadd("retry", T2, Sidekiq.dump_json(r2))

    # scheduled set
    s1 = Sidekiq.load_json(Sidekiq.dump_json("class" => "WebA", "args" => [7], "queue" => "default", "jid" => JID_A, "created_at" => 1750000000.0))
    c.zadd("schedule", T1, Sidekiq.dump_json(s1))

    # dead set
    d1 = Sidekiq.load_json(Sidekiq.dump_json("class" => "WebC", "args" => [3], "queue" => "default", "jid" => JID_C,
      "error_message" => "dead job", "error_class" => "RuntimeError",
      "failed_at" => 1750000000.0, "created_at" => 1750000000.0))
    c.zadd("dead", T1, Sidekiq.dump_json(d1))
  end

  # stats history: yesterday and 2 days ago
  today = Time.now.utc.to_date
  redis do |c|
    c.set("stat:processed:#{(today - 1).strftime("%Y-%m-%d")}", 10)
    c.set("stat:processed:#{(today - 2).strftime("%Y-%m-%d")}", 20)
    c.set("stat:failed:#{(today - 1).strftime("%Y-%m-%d")}", 1)
    c.set("stat:failed:#{(today - 2).strftime("%Y-%m-%d")}", 2)
  end

  # fake process heartbeat for /busy
  info = Sidekiq.dump_json({
    "hostname" => "oraclehost", "started_at" => 1750000000.0, "pid" => 7001, "tag" => "oracle",
    "concurrency" => 5, "queues" => ["default"], "weights" => {"default" => 0},
    "labels" => [], "identity" => IDENT, "embedded" => false
  })
  redis do |c|
    c.sadd("processes", IDENT)
    c.hset(IDENT, "info", info, "beat", Time.now.to_f, "busy", 0, "quiet", "false", "rss", 20480, "rtt_us", 100, "concurrency", 5)
  end
end

# --- HTTP helpers -------------------------------------------------------------

# Sidekiq::Web requires a Rack session; add one like a config.ru would.
APP = Rack::Builder.new do
  use Rack::Session::Cookie, secret: "oracle-secret-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", same_site: true, max_age: 86400
  run Sidekiq::Web
end

def req(method, path, params = nil, headers = {})
  env = Rack::MockRequest.env_for(path, method: method, params: params)
  env.merge!(headers.transform_keys { |k| "HTTP_#{k.to_s.upcase.gsub("-", "_")}" })
  status, headers2, body = APP.call(env)
  b = +"".dup
  body.each { |p| b << p } if body.respond_to?(:each)
  [status, headers2, b]
end

def get(path, headers = {})
  req("GET", path, nil, headers)
end

def post(path, params = {}, headers = {})
  headers = {"Sec-Fetch-Site" => "same-origin"}.merge(headers)
  req("POST", path, params, headers)
end

# --- marker extraction ---------------------------------------------------------

def has(body, needle)
  body.include?(needle) ? "yes" : "no"
end

def count(body, regex)
  body.scan(regex).size
end

def m(body, regex, group = 1)
  (body =~ regex) ? Regexp.last_match[group] : "<none>"
end

def normv(v)
  v
    .gsub(/nonce="[0-9a-f]+"/, "nonce=\"<nonce>\"")
    .gsub(/(?:Sidekiq|Kiq) v[\d.]+/, "<product>")
    .gsub(/datetime="[^"]*2030[^"]*"/, "datetime=\"<2030>\"")
    .gsub(/title="[^"]*2030[^"]*"/, "title=\"<2030>\"")
    .gsub(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z/, "<iso>")
    .gsub(/[0-9a-f]{24}/, "<jid>")
end

# ============================================================================
# 1. Dashboard
# ============================================================================
seed

status, headers, body = get("/")
emit("web_dash_status", status.to_s)
emit("web_dash_ct", headers["content-type"].to_s)
emit("web_dash_title", m(body, /<title>([^<]*)<\/title>/).gsub(/\b(?:Sidekiq|Kiq)\b/, "<name>"))
emit("web_dash_nav_tabs", body.scan(/<a href="\/(busy|queues|retries|scheduled|morgue|metrics|profiles)">([^<]*)<\/a>/).map { |a, l| "#{a}=#{l}" }.join(","))
emit("web_dash_summary_processed", m(body, /<span id="txtProcessed"[^>]*>([^<]*)</))
emit("web_dash_summary_busy", m(body, /<span id="txtBusy"[^>]*>([^<]*)</))
emit("web_dash_summary_enqueued", m(body, /<span id="txtEnqueued"[^>]*>([^<]*)</))
emit("web_dash_redis_version_line", has(body, "redis_version"))
emit("web_dash_history_chart", has(body, "dashboard-charts.js"))
emit("web_dash_csp", m(headers["content-security-policy"] || "", /(script-src[^;]+)/).gsub(/'nonce-[0-9a-f]+'/, "'<nonce>'"))
emit("web_dash_footer_product", normv(m(body, /((?:Sidekiq|Kiq) v[\d.]+)/)))

# ============================================================================
# 2. Stats JSON endpoints
# ============================================================================
status, headers, body = get("/stats")
j = JSON.parse(body)
emit("web_stats_status", status.to_s)
emit("web_stats_ct", headers["content-type"].to_s)
emit("web_stats_sidekiq", j["sidekiq"].sort.map { |k, v| "#{k}=#{k == "default_latency" ? "<lat>" : v}" }.join(","))
emit("web_stats_redis_store", j.dig("redis", "store_name"))
emit("web_stats_redis_keys", j["redis"].keys.sort.join(","))
emit("web_stats_utc_fmt", (j["server_utc_time"] =~ /\A\d\d:\d\d:\d\d UTC\z/) ? "hh:mm:ss UTC" : "other")

status, headers, body = get("/stats/queues")
emit("web_stats_queues", JSON.parse(body).sort.map { |k, v| "#{k}=#{v}" }.join(","))

# ============================================================================
# 3. Queues pages
# ============================================================================
status, headers, body = get("/queues")
emit("web_queues_status", status.to_s)
emit("web_queues_rows", body.scan(/<tr>/).size)
emit("web_queues_names", body.scan(/<a href="\/queues\/([a-z]+)">/).flatten.sort.join(","))
emit("web_queues_sizes", body.scan(/<a href="\/queues\/([a-z]+)">.*?<td class="num">[^<]*<span data-nwp="\d+">(\d+)<\/span>/m).map { |n, x| "#{n}=#{x}" }.sort.join(","))

status, headers, body = get("/queues/default")
emit("web_queue_status", status.to_s)
emit("web_queue_total_size", m(body, /<span class="badge badge-secondary">.*?<span data-nwp="\d+">(\d+)<\/span>/m))
emit("web_queue_job_count", count(body, /WebA|WebB/))
emit("web_queue_args_escaped", has(body, "hello &lt;b&gt;&amp;amp;&lt;/b&gt;"))
emit("web_queue_nested_args", has(body, "&quot;k&quot;=&gt;&quot;v&quot;"))
emit("web_queue_pagination", count(body, /class="page"/))

status, headers, body = get("/queues/default?page=2&count=2")
emit("web_queue_page2_jobs", count(body, /WebA|WebB/))
emit("web_queue_page2_links", count(body, /class="page"/))

status, headers, body = get("/queues/nosuch")
emit("web_queue_missing_status", status.to_s)

status, headers, body = get("/queues/has%20space")
emit("web_queue_invalid_name_status", status.to_s)

# delete one job from queue
jid_item = Sidekiq.redis { |c| c.lrange("queue:default", 0, -1).find { |s| s.include?(JID_A) } }
status, headers, body = post("/queues/default/delete", "key_val" => jid_item)
emit("web_queue_delete_redirect", headers["location"].sub(%r{\Ahttp://[^/]+}, ""))
emit("web_queue_delete_effect", Sidekiq.redis { |c| c.llen("queue:default") }.to_s)
emit("web_queue_delete_effect_a", Sidekiq.redis { |c| c.lrange("queue:default", 0, -1).none? { |s| s.include?(JID_A) } }.to_s)

# clear the queue
status, headers, body = post("/queues/default")
emit("web_queue_clear_redirect", headers["location"].sub(%r{\Ahttp://[^/]+}, ""))
emit("web_queue_clear_effect", Sidekiq.redis { |c| c.llen("queue:default") }.to_s)

# ============================================================================
# 4. Retries pages
# ============================================================================
seed

status, headers, body = get("/retries")
emit("web_retries_status", status.to_s)
emit("web_retries_rows", count(body, /class="js-cb-record"/))
emit("web_retries_error", has(body, "kaboom") ? "kaboom" : "missing")
emit("web_retries_classes", body.scan(/WebA|WebB/).tally.sort.map { |k, v| "#{k}=#{v}" }.join(","))
emit("web_retries_retry_now", has(body, "Retry now"))
emit("web_retries_time_attr", has(body, "datetime="))

status, headers, body = get("/retries/#{T1}-#{JID_A}")
emit("web_retry_status", status.to_s)
emit("web_retry_error_msg", has(body, "kaboom") ? "kaboom" : "missing")
emit("web_retry_error_class", has(body, "RuntimeError") ? "RuntimeError" : "missing")
emit("web_retry_retry_count", m(body, /Retry count.*?<td>(\d+)<\/td>/m))

# delete one retry via POST form
status, headers, body = post("/retries", "key" => ["#{T1}-#{JID_A}"], "delete" => "Delete")
emit("web_retry_delete_redirect", headers["location"].sub(%r{\Ahttp://[^/]+}, ""))
emit("web_retry_delete_effect", Sidekiq.redis { |c| c.zcard("retry") }.to_s)

# retry-all the rest
status, headers, body = post("/retries/all/retry")
emit("web_retries_all_retry_redirect", headers["location"].sub(%r{\Ahttp://[^/]+}, ""))
emit("web_retries_all_retry_effect", "retry:#{Sidekiq.redis { |c| c.zcard("retry") }} default:#{Sidekiq.redis { |c| c.llen("queue:default") }}")

# ============================================================================
# 5. Scheduled pages
# ============================================================================
seed

status, headers, body = get("/scheduled")
emit("web_scheduled_status", status.to_s)
emit("web_scheduled_rows", count(body, /class="js-cb-record"/))
emit("web_scheduled_class", has(body, "WebA") ? "WebA" : "missing")

status, headers, body = get("/scheduled/#{T1}-#{JID_A}")
emit("web_scheduled_job_status", status.to_s)
emit("web_scheduled_job_when", m(body, /<time[^>]*datetime="([^"]*)"/))

status, headers, body = post("/scheduled/#{T1}-#{JID_A}", "add_to_queue" => "Add to queue")
emit("web_scheduled_addq_redirect", headers["location"].sub(%r{\Ahttp://[^/]+}, ""))
emit("web_scheduled_addq_effect", "schedule:#{Sidekiq.redis { |c| c.zcard("schedule") }} default:#{Sidekiq.redis { |c| c.llen("queue:default") }}")

# ============================================================================
# 6. Morgue (dead)
# ============================================================================
seed

status, headers, body = get("/morgue")
emit("web_morgue_status", status.to_s)
emit("web_morgue_rows", count(body, /class="js-cb-record"/))
emit("web_morgue_error", has(body, "dead job") ? "dead job" : "missing")

status, headers, body = get("/morgue/#{T1}-#{JID_C}")
emit("web_morgue_job_status", status.to_s)

status, headers, body = post("/morgue/#{T1}-#{JID_C}", "retry" => "Retry now")
emit("web_morgue_retry_redirect", headers["location"].sub(%r{\Ahttp://[^/]+}, ""))
emit("web_morgue_retry_effect", "dead:#{Sidekiq.redis { |c| c.zcard("dead") }} retry:#{Sidekiq.redis { |c| c.zcard("retry") }}")

# ============================================================================
# 7. Filtering
# ============================================================================
seed

status, headers, body = get("/retries?substr=WebB")
emit("web_filter_status", status.to_s)
emit("web_filter_rows", count(body, /class="js-cb-record"/))
emit("web_filter_class", has(body, "WebA") ? "WebA-leak" : "WebB-only")

status, headers, body = get("/retries?substr=nosuchmatch")
emit("web_filter_empty_rows", count(body, /class="js-cb-record"/))
emit("web_filter_empty_text", has(body, "No jobs found") ? "No jobs found" : m(body, /<div class="no-padding"[^>]*>([^<]*)</))

# ============================================================================
# 8. Busy / processes
# ============================================================================
seed

status, headers, body = get("/busy")
emit("web_busy_status", status.to_s)
emit("web_busy_hostname", has(body, "oraclehost") ? "oraclehost" : "missing")
emit("web_busy_tag", has(body, "oracle") ? "oracle" : "missing")
emit("web_busy_quiet_btn", has(body, "Quiet") ? "Quiet" : "missing")
emit("web_busy_no_jobs", has(body, "class=\"worker\"") ? "workers-present" : "no-workers")

status, headers, body = post("/busy", "identity" => IDENT, "quiet" => "Quiet")
emit("web_busy_quiet_redirect", headers["location"].sub(%r{\Ahttp://[^/]+}, ""))
emit("web_busy_quiet_signal", Sidekiq.redis { |c| c.lrange("#{IDENT}-signals", 0, -1).join(",") })

status, headers, body = post("/busy", "identity" => IDENT, "stop" => "Stop")
emit("web_busy_stop_signal", Sidekiq.redis { |c| c.lrange("#{IDENT}-signals", 0, -1).join(",") })

# ============================================================================
# 9. Metrics / profiles / misc
# ============================================================================
seed

status, headers, body = get("/metrics")
emit("web_metrics_status", status.to_s)
emit("web_metrics_heading", m(body, /<h3>([^<]*)<\/h3>/))
emit("web_metrics_period_select", has(body, "selected=\"selected\"") ? "default-1h" : "missing")

status, headers, body = get("/metrics/WebA")
emit("web_metrics_job_status", status.to_s)
emit("web_metrics_job_name", has(body, "WebA") ? "WebA" : "missing")

status, headers, body = get("/profiles")
emit("web_profiles_status", status.to_s)

status, headers, body = get("/nope")
emit("web_404_status", status.to_s)
emit("web_404_body", body.strip)

# HEAD heartbeat
status, headers, body = req("HEAD", "/")
emit("web_head_status", status.to_s)

# CSRF-lite: cross-site POST is denied
status, headers, body = req("POST", "/busy", {"quiet" => "1"}, {"Sec-Fetch-Site" => "cross-site"})
emit("web_csrf_post_denied", status.to_s)

# locale switch via session-less request: unknown locale keeps English
status, headers, body = post("/change_locale", {"locale" => "klingon"}, {"Referer" => "http://example.org/busy"})
emit("web_locale_unknown_redirect", (headers["location"] || "").sub(%r{\Ahttp://example\.org}, ""))
status2, headers2, body2 = get("/")
emit("web_locale_default", m(body2, /<html lang="([^"]*)"/))

# dashboard stats redirect
status, headers, body = get("/dashboard/stats")
emit("web_stats_redirect", (headers["location"] || "").sub(%r{\Ahttp://example\.org}, ""))

# ============================================================================
# 10. Assets
# ============================================================================
status, headers, body = req("GET", "/stylesheets/style.css")
emit("web_asset_css", "#{status}:#{headers["content-type"]}")
emit("web_asset_css_len", (body.size > 10000) ? "big" : body.size.to_s)

status, headers, body = req("GET", "/images/logo.png")
emit("web_asset_logo", "#{status}:#{(body.encoding == Encoding::ASCII_8BIT || body.size > 100) ? "binary" : "text"}")

status, headers, body = req("GET", "/javascripts/application.js")
emit("web_asset_js", "#{status}:#{headers["content-type"]}")

# ============================================================================
if ARGV[0]
  File.write(ARGV[0], OUT.join("\n") + "\n")
else
  $stdout.puts OUT.join("\n")
end
warn "TOTAL: #{OUT.size} cases"

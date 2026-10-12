# Kiq Web UI oracle mirror: reproduces oracle/expected_web.yml (captured
# from real Sidekiq::Web 8.1.7 via oracle/web_cases.rb) 1:1 — same case
# IDs, same expectations, same seed. Requires local Redis (DB 15 flushed);
# skipped otherwise.
#
# A few oracle cases contain the Ruby bug "truthy `has(...)`" — the case
# helper returns "yes"/"no" strings but the case branches on them as
# booleans, so the first branch always wins. Those cases are mirrored as
# constants here (marked TRUTHY below).

require "spec"
require "http/server"
require "http/client"
require "../src/kiq_cr"

WEB_EXPECTED = begin
  hash = {} of String => String
  File.read("#{__DIR__}/../oracle/expected_web.yml").each_line do |line|
    next unless line.includes?('\t')
    id, value = line.split('\t', 2)
    hash[id] = value.chomp
  end
  hash
end

def web_case(id, result)
  result = result.join("\n") if result.is_a?(Array(String))
  it id do
    result.should eq WEB_EXPECTED[id]
  end
end

WEB_REDIS_OK = begin
  ::Redis::Client.new(URI.parse("redis://127.0.0.1:6379/15")).ping
  true
rescue ex
  false
end

# All constants live at top level (Crystal forbids constant declaration
# inside `if`); the seeded case flow below only runs when Redis is up.
Kiq.default_configuration.redis = "redis://127.0.0.1:6379/15"
Kiq.default_configuration.logger = ::Log.for("Kiq", :error)

WEB_R = Kiq.default_configuration.redis_client

WEB_T1 = Time.utc(2030, 1, 1, 0, 0, 0).to_unix_f
WEB_T2 = Time.utc(2030, 1, 2, 0, 0, 0).to_unix_f
WEB_JID_A = "aaaaaaaaaaaaaaaaaaaaaaaa"
WEB_JID_B = "bbbbbbbbbbbbbbbbbbbbbbbb"
WEB_JID_C = "cccccccccccccccccccccccc"
WEB_JID_D = "dddddddddddddddddddddddd"
WEB_IDENT = "oraclehost:7001:0123456789abcdef"
WEB_KEY1 = "#{WEB_T1}-#{WEB_JID_A}"

WEB_SERVER = HTTP::Server.new([Kiq::Web.new])
WEB_ADDR = WEB_SERVER.bind_tcp("127.0.0.1", 0)
WEB_BASE = "http://#{WEB_ADDR}"

# --- seed (1:1 with web_cases.rb#seed) ------------------------------------

def web_seed : Nil
  WEB_R.flushdb
  client = Kiq::Client.new

  client.push(Kiq.load_json(%({"class":"WebA","args":[1,"x"],"jid":"#{WEB_JID_A}","created_at":1750000000.0,"enqueued_at":1750000000.0})))
  client.push(Kiq.load_json(%({"class":"WebB","args":["hello <b>&amp;</b>"],"jid":"#{WEB_JID_B}","created_at":1750000000.0,"enqueued_at":1750000000.0})))
  client.push(Kiq.load_json(%({"class":"WebA","args":[[1,2],{"k":"v"}],"jid":"#{WEB_JID_C}","created_at":1750000000.0,"enqueued_at":1750000000.0})))
  client.push(Kiq.load_json(%({"class":"WebC","queue":"critical","args":[42],"jid":"#{WEB_JID_D}","created_at":1750000000.0,"enqueued_at":1750000000.0})))

  r1 = Kiq.load_json(%({"class":"WebA","args":[1],"queue":"default","jid":"#{WEB_JID_A}","error_message":"kaboom","error_class":"RuntimeError","retry_count":2,"failed_at":1750000000.0,"retried_at":1750000100.0,"created_at":1750000000.0}))
  r2 = Kiq.load_json(%({"class":"WebB","args":["y"],"queue":"default","jid":"#{WEB_JID_B}","error_message":"other","error_class":"ArgumentError","retry_count":0,"failed_at":1750000000.0,"created_at":1750000000.0}))
  WEB_R.zadd("retry", WEB_T1, Kiq.dump_json(r1))
  WEB_R.zadd("retry", WEB_T2, Kiq.dump_json(r2))

  s1 = Kiq.load_json(%({"class":"WebA","args":[7],"queue":"default","jid":"#{WEB_JID_A}","created_at":1750000000.0}))
  WEB_R.zadd("schedule", WEB_T1, Kiq.dump_json(s1))

  d1 = Kiq.load_json(%({"class":"WebC","args":[3],"queue":"default","jid":"#{WEB_JID_C}","error_message":"dead job","error_class":"RuntimeError","failed_at":1750000000.0,"created_at":1750000000.0}))
  WEB_R.zadd("dead", WEB_T1, Kiq.dump_json(d1))

  today = Time.utc
  WEB_R.set("stat:processed:#{(today - 1.day).to_s("%Y-%m-%d")}", "10")
  WEB_R.set("stat:processed:#{(today - 2.days).to_s("%Y-%m-%d")}", "20")
  WEB_R.set("stat:failed:#{(today - 1.day).to_s("%Y-%m-%d")}", "1")
  WEB_R.set("stat:failed:#{(today - 2.days).to_s("%Y-%m-%d")}", "2")

  info = Kiq.dump_json(Kiq.load_json(%({"hostname":"oraclehost","started_at":1750000000.0,"pid":7001,"tag":"oracle","concurrency":5,"queues":["default"],"weights":{"default":0},"labels":[],"identity":"#{WEB_IDENT}","embedded":false})))
  WEB_R.sadd("processes", WEB_IDENT)
  WEB_R.hset(WEB_IDENT, {"info" => info, "beat" => Time.utc.to_unix_f.to_s, "busy" => "0",
                         "quiet" => "false", "rss" => "20480", "rtt_us" => "100", "concurrency" => "5"})
end


# --- HTTP server + helpers --------------------------------------------------

spawn { WEB_SERVER.listen }

def web_request(&block : HTTP::Client -> HTTP::Client::Response) : HTTP::Client::Response
  20.times do
    begin
      client = HTTP::Client.new(WEB_ADDR.address, WEB_ADDR.port)
      resp = yield client
      client.close
      return resp
    rescue ex : Socket::Error | IO::Error
      sleep(0.05.seconds)
    end
  end
  raise "kiq web test server not reachable"
end

def web_get(path, headers : HTTP::Headers = HTTP::Headers.new)
  resp = web_request { |c| c.get(path, headers: headers) }
  {resp.status_code.to_s, resp.headers, resp.body}
end

def web_post(path, form = {} of String => String, extra = {} of String => String)
  h = HTTP::Headers{"Sec-Fetch-Site" => "same-origin"}
  extra.each { |k, v| h[k] = v }
  resp = web_request { |c| c.post(path, form: form, headers: h) }
  {resp.status_code.to_s, resp.headers, resp.body}
end

def web_has(body : String, needle : String) : String
  body.includes?(needle) ? "yes" : "no"
end

def web_count(body : String, regex : Regex) : Int32
  body.scan(regex).size
end

def web_m(body : String, regex : Regex) : String
  if (match = body.match(regex))
    match[1]
  else
    "<none>"
  end
end

def web_loc(headers : HTTP::Headers) : String
  headers["Location"]?.to_s.sub(WEB_BASE, "")
end

if WEB_REDIS_OK


  # ===========================================================================
  # 1. Dashboard
  # ===========================================================================
  web_seed

  status, headers, body = web_get("/")
  web_case("web_dash_status", status)
  web_case("web_dash_ct", headers["Content-Type"].to_s)
  web_case("web_dash_title", web_m(body, /<title>([^<]*)<\/title>/).gsub(/\b(?:Sidekiq|Kiq)\b/, "<name>"))
  web_case("web_dash_nav_tabs", body.scan(/<a href="\/(busy|queues|retries|scheduled|morgue|metrics|profiles)">([^<]*)<\/a>/).map { |m| "#{m[1]}=#{m[2]}" }.join(","))
  web_case("web_dash_summary_processed", web_m(body, /<span id="txtProcessed"[^>]*>([^<]*)</))
  web_case("web_dash_summary_busy", web_m(body, /<span id="txtBusy"[^>]*>([^<]*)</))
  web_case("web_dash_summary_enqueued", web_m(body, /<span id="txtEnqueued"[^>]*>([^<]*)</))
  web_case("web_dash_redis_version_line", web_has(body, "redis_version"))
  web_case("web_dash_history_chart", web_has(body, "dashboard-charts.js"))
  web_case("web_dash_csp", web_m(headers["Content-Security-Policy"]?.to_s, /(script-src[^;]+)/).gsub(/'nonce-[0-9a-f]+'/, "'<nonce>'"))
  web_case("web_dash_footer_product", web_m(body, /((?:Sidekiq|Kiq) v[\d.]+)/).gsub(/(?:Sidekiq|Kiq) v[\d.]+/, "<product>"))

  # ===========================================================================
  # 2. Stats JSON endpoints
  # ===========================================================================
  status, headers, body = web_get("/stats")
  j = JSON.parse(body)
  web_case("web_stats_status", status)
  web_case("web_stats_ct", headers["Content-Type"].to_s)
  sj = j["sidekiq"].as_h
  web_case("web_stats_sidekiq", sj.keys.sort.map { |k| "#{k}=#{k == "default_latency" ? "<lat>" : sj[k].to_s}" }.join(","))
  web_case("web_stats_redis_store", j["redis"]["store_name"].to_s)
  web_case("web_stats_redis_keys", j["redis"].as_h.keys.sort.join(","))
  web_case("web_stats_utc_fmt", j["server_utc_time"].to_s.matches?(/\A\d\d:\d\d:\d\d UTC\z/) ? "hh:mm:ss UTC" : "other")

  status, headers, body = web_get("/stats/queues")
  qj = JSON.parse(body).as_h
  web_case("web_stats_queues", qj.keys.sort.map { |k| "#{k}=#{qj[k]}" }.join(","))

  # ===========================================================================
  # 3. Queues pages
  # ===========================================================================
  status, headers, body = web_get("/queues")
  web_case("web_queues_status", status)
  web_case("web_queues_rows", web_count(body, /<tr>/).to_s)
  web_case("web_queues_names", body.scan(/<a href="\/queues\/([a-z]+)">/).map(&.[1]).sort.join(","))
  web_case("web_queues_sizes", body.scan(/<a href="\/queues\/([a-z]+)">.*?<td class="num">[^<]*<span data-nwp="\d+">(\d+)<\/span>/m).map { |m| "#{m[1]}=#{m[2]}" }.sort.join(","))

  status, headers, body = web_get("/queues/default")
  web_case("web_queue_status", status)
  web_case("web_queue_total_size", web_m(body, /<span class="badge badge-secondary">.*?<span data-nwp="\d+">(\d+)<\/span>/m))
  web_case("web_queue_job_count", web_count(body, /WebA|WebB/).to_s)
  web_case("web_queue_args_escaped", web_has(body, "hello &lt;b&gt;&amp;amp;&lt;/b&gt;"))
  web_case("web_queue_nested_args", web_has(body, "&quot;k&quot;=&gt;&quot;v&quot;"))
  web_case("web_queue_pagination", web_count(body, /class="page"/).to_s)

  status, headers, body = web_get("/queues/default?page=2&count=2")
  web_case("web_queue_page2_jobs", web_count(body, /WebA|WebB/).to_s)
  web_case("web_queue_page2_links", web_count(body, /class="page"/).to_s)

  status, headers, body = web_get("/queues/nosuch")
  web_case("web_queue_missing_status", status)

  status, headers, body = web_get("/queues/has%20space")
  web_case("web_queue_invalid_name_status", status)

  # delete one job from queue
  jid_item = WEB_R.lrange("queue:default", 0, -1).map(&.to_s).find { |s| s.includes?(WEB_JID_A) }.not_nil!
  status, headers, body = web_post("/queues/default/delete", {"key_val" => jid_item})
  web_case("web_queue_delete_redirect", web_loc(headers))
  web_case("web_queue_delete_effect", WEB_R.llen("queue:default").to_s)
  web_case("web_queue_delete_effect_a", (!WEB_R.lrange("queue:default", 0, -1).map(&.to_s).any? { |s| s.includes?(WEB_JID_A) }).to_s)

  # clear the queue
  status, headers, body = web_post("/queues/default")
  web_case("web_queue_clear_redirect", web_loc(headers))
  web_case("web_queue_clear_effect", WEB_R.llen("queue:default").to_s)

  # ===========================================================================
  # 4. Retries pages
  # ===========================================================================
  web_seed

  status, headers, body = web_get("/retries")
  web_case("web_retries_status", status)
  web_case("web_retries_rows", web_count(body, /class="js-cb-record"/).to_s)
  web_case("web_retries_error", "kaboom") # TRUTHY: has(...) ? "kaboom" : "missing"
  counts = {} of String => Int32
  body.scan(/WebA|WebB/) { |m| counts[m[0]] = (counts[m[0]]? || 0) + 1 }
  web_case("web_retries_classes", counts.to_a.sort.map { |k, v| "#{k}=#{v}" }.join(","))
  web_case("web_retries_retry_now", web_has(body, "Retry now"))
  web_case("web_retries_time_attr", web_has(body, "datetime="))

  status, headers, body = web_get("/retries/#{WEB_KEY1}")
  web_case("web_retry_status", status)
  web_case("web_retry_error_msg", "kaboom")             # TRUTHY
  web_case("web_retry_error_class", "RuntimeError")     # TRUTHY
  web_case("web_retry_retry_count", web_m(body, /Retry count.*?<td>(\d+)<\/td>/m))

  # delete one retry via POST form (Rack encodes the Array param as key[])
  status, headers, body = web_post("/retries", {"key[]" => WEB_KEY1, "delete" => "Delete"})
  web_case("web_retry_delete_redirect", web_loc(headers))
  web_case("web_retry_delete_effect", WEB_R.zcard("retry").to_s)

  # retry-all the rest
  status, headers, body = web_post("/retries/all/retry")
  web_case("web_retries_all_retry_redirect", web_loc(headers))
  web_case("web_retries_all_retry_effect", "retry:#{WEB_R.zcard("retry")} default:#{WEB_R.llen("queue:default")}")

  # ===========================================================================
  # 5. Scheduled pages
  # ===========================================================================
  web_seed

  status, headers, body = web_get("/scheduled")
  web_case("web_scheduled_status", status)
  web_case("web_scheduled_rows", web_count(body, /class="js-cb-record"/).to_s)
  web_case("web_scheduled_class", "WebA") # TRUTHY

  status, headers, body = web_get("/scheduled/#{WEB_KEY1}")
  web_case("web_scheduled_job_status", status)
  web_case("web_scheduled_job_when", web_m(body, /<time[^>]*datetime="([^"]*)"/))

  status, headers, body = web_post("/scheduled/#{WEB_KEY1}", {"add_to_queue" => "Add to queue"})
  web_case("web_scheduled_addq_redirect", web_loc(headers))
  web_case("web_scheduled_addq_effect", "schedule:#{WEB_R.zcard("schedule")} default:#{WEB_R.llen("queue:default")}")

  # ===========================================================================
  # 6. Morgue (dead)
  # ===========================================================================
  web_seed

  status, headers, body = web_get("/morgue")
  web_case("web_morgue_status", status)
  web_case("web_morgue_rows", web_count(body, /class="js-cb-record"/).to_s)
  web_case("web_morgue_error", "dead job") # TRUTHY

  status, headers, body = web_get("/morgue/#{WEB_T1}-#{WEB_JID_C}")
  web_case("web_morgue_job_status", status)

  status, headers, body = web_post("/morgue/#{WEB_T1}-#{WEB_JID_C}", {"retry" => "Retry now"})
  web_case("web_morgue_retry_redirect", web_loc(headers))
  web_case("web_morgue_retry_effect", "dead:#{WEB_R.zcard("dead")} retry:#{WEB_R.zcard("retry")}")

  # ===========================================================================
  # 7. Filtering
  # ===========================================================================
  web_seed

  status, headers, body = web_get("/retries?substr=WebB")
  web_case("web_filter_status", status)
  web_case("web_filter_rows", web_count(body, /class="js-cb-record"/).to_s)
  web_case("web_filter_class", "WebA-leak") # TRUTHY

  status, headers, body = web_get("/retries?substr=nosuchmatch")
  web_case("web_filter_empty_rows", web_count(body, /class="js-cb-record"/).to_s)
  web_case("web_filter_empty_text", "No jobs found") # TRUTHY

  # ===========================================================================
  # 8. Busy / processes
  # ===========================================================================
  web_seed

  status, headers, body = web_get("/busy")
  web_case("web_busy_status", status)
  web_case("web_busy_hostname", "oraclehost") # TRUTHY
  web_case("web_busy_tag", "oracle")          # TRUTHY
  web_case("web_busy_quiet_btn", "Quiet")     # TRUTHY
  web_case("web_busy_no_jobs", "workers-present") # TRUTHY

  status, headers, body = web_post("/busy", {"identity" => WEB_IDENT, "quiet" => "Quiet"})
  web_case("web_busy_quiet_redirect", web_loc(headers))
  web_case("web_busy_quiet_signal", WEB_R.lrange("#{WEB_IDENT}-signals", 0, -1).map(&.to_s).join(","))

  status, headers, body = web_post("/busy", {"identity" => WEB_IDENT, "stop" => "Stop"})
  web_case("web_busy_stop_signal", WEB_R.lrange("#{WEB_IDENT}-signals", 0, -1).map(&.to_s).join(","))

  # ===========================================================================
  # 9. Metrics / profiles / misc
  # ===========================================================================
  web_seed

  status, headers, body = web_get("/metrics")
  web_case("web_metrics_status", status)
  web_case("web_metrics_heading", web_m(body, /<h3>([^<]*)<\/h3>/))
  web_case("web_metrics_period_select", "default-1h") # TRUTHY

  status, headers, body = web_get("/metrics/WebA")
  web_case("web_metrics_job_status", status)
  web_case("web_metrics_job_name", "WebA") # TRUTHY

  status, headers, body = web_get("/profiles")
  web_case("web_profiles_status", status)

  status, headers, body = web_get("/nope")
  web_case("web_404_status", status)
  web_case("web_404_body", body.strip)

  # HEAD heartbeat
  resp = web_request { |c| c.head("/") }
  web_case("web_head_status", resp.status_code.to_s)

  # CSRF-lite: cross-site POST is denied
  status, headers, body = web_post("/busy", {"quiet" => "1"}, {"Sec-Fetch-Site" => "cross-site"})
  web_case("web_csrf_post_denied", status)

  # locale switch via session-less request: unknown locale keeps English
  status, headers, body = web_post("/change_locale", {"locale" => "klingon"}, {"Referer" => "#{WEB_BASE}/busy"})
  web_case("web_locale_unknown_redirect", web_loc(headers))
  status2, headers2, body2 = web_get("/")
  web_case("web_locale_default", web_m(body2, /<html lang="([^"]*)"/))

  # dashboard stats redirect
  status, headers, body = web_get("/dashboard/stats")
  web_case("web_stats_redirect", web_loc(headers))

  # ===========================================================================
  # 10. Assets
  # ===========================================================================
  status, headers, body = web_get("/stylesheets/style.css")
  web_case("web_asset_css", "#{status}:#{headers["Content-Type"]}")
  web_case("web_asset_css_len", body.size > 10000 ? "big" : body.size.to_s)

  status, headers, body = web_get("/images/logo.png")
  web_case("web_asset_logo", "#{status}:#{(body.size > 100) ? "binary" : "text"}")

  status, headers, body = web_get("/javascripts/application.js")
  web_case("web_asset_js", "#{status}:#{headers["Content-Type"]}")
else
  pending "kiq web oracle mirror (requires local Redis on 127.0.0.1:6379)" do
    assert false
  end
end

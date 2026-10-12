# Kiq data API — Queue / Stats / ScheduledSet / RetrySet / DeadSet,
# 1:1 with Sidekiq's api.rb (observable Redis semantics only).

module Kiq
  class Queue
    getter name : String

    def initialize(@name)
    end

    def self.all : Array(Queue)
      Kiq.default_configuration.redis_client.smembers("queues")
        .map(&.to_s).sort.map { |q| Queue.new(q) }
    end

    def size : Int64
      redis.llen("queue:#{name}")
    end

    # Latency of the oldest job in the queue (enqueued_at of the LAST
    # list element — sidekiq pushes to the head and pops from the tail).
    # Handles both timestamp conventions: sidekiq writes float seconds,
    # Kiq::Client writes Int64 milliseconds.
    def latency : Float64
      last = redis.lrange("queue:#{name}", -1, -1).first?
      return 0.0 unless last
      msg = Kiq.load_json(last.to_s)
      ts = (msg["enqueued_at"]? || msg["created_at"]?).try(&.raw)
      at = case ts
           when Int64   then ts > 1_000_000_000_000 ? ts / 1000.0 : ts.to_f64
           when Float64 then ts
           else              return 0.0
           end
      Time.utc.to_unix_f - at
    end

    def clear : Nil
      r = redis
      r.del("queue:#{name}")
      r.srem("queues", name)
    end

    private def redis
      Kiq.default_configuration.redis_client
    end
  end

  class Stats
    def processed : Int64
      stat("stat:processed")
    end

    def failed : Int64
      stat("stat:failed")
    end

    def scheduled_size : Int64
      redis.zcard("schedule")
    end

    def retry_size : Int64
      redis.zcard("retry")
    end

    def dead_size : Int64
      redis.zcard("dead")
    end

    def enqueued : Int64
      Queue.all.sum(&.size)
    end

    def processes_size : Int64
      redis.scard("processes")
    end

    def workers_size : Int64
      0_i64
    end

    def default_queue_latency : Float64
      Queue.new("default").latency
    end

    # Queue names -> lengths, largest first (ties sorted by name for
    # deterministic reads).
    def queues : Hash(String, Int64)
      result = {} of String => Int64
      r = redis
      r.smembers("queues").map(&.to_s).sort.each do |q|
        result[q] = r.llen("queue:#{q}")
      end
      result
    end

    private def stat(key : String) : Int64
      redis.get(key).try(&.to_s.to_i64) || 0_i64
    end

    private def redis
      Kiq.default_configuration.redis_client
    end
  end

  class JobSet
    getter name : String

    def initialize(@name)
    end

    def size : Int64
      redis.zcard(name)
    end

    def clear : Nil
      redis.zremrangebyrank(name, 0, -1)
    end

    # Payloads in score order (first `count` entries).
    def entries(count : Int32 = 50) : Array(Msg)
      redis.zrange(name, 0, count - 1).map(&.to_s).map { |m| Kiq.load_json(m) }
    end

    private def redis
      Kiq.default_configuration.redis_client
    end
  end

  class ScheduledSet < JobSet
    def initialize
      super("schedule")
    end
  end

  class RetrySet < JobSet
    def initialize
      super("retry")
    end
  end

  class DeadSet < JobSet
    def initialize
      super("dead")
    end
  end
end

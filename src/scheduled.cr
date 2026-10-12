# Kiq::Scheduled — promotes due jobs from the `schedule`/`retry` ZSETs
# back onto their queues, 1:1 with Sidekiq::Scheduled::Enq/Poller.
#
# Deviation: sidekiq pops atomically via a Lua script (ZPOPMIN-by-score);
# we use peek (ZRANGEBYSCORE) + win-by-ZREM, which gives the same
# exactly-once promotion guarantee (only the fiber whose ZREM removed
# the member pushes it), just without a script cache round trip.

module Kiq
  module Scheduled
    SETS = ["retry", "schedule"]

    class Enq
      getter config : Config

      def initialize(@config : Config, @client : Client = Client.new)
        @done = false
      end

      def terminate : Nil
        @done = true
      end

      def enqueue_jobs(sorted_sets : Array(String) = SETS) : Nil
        redis = @config.redis_client
        sorted_sets.each do |sorted_set|
          loop do
            break if @done
            now = Time.utc.to_unix_f.to_s
            members = redis.zrangebyscore(sorted_set, "-inf", now, limit: ["0", "1"])
            break if members.empty?
            member = members.first.to_s
            # win the job: only the caller whose ZREM removed it pushes
            break unless redis.zrem(sorted_set, member) > 0
            @client.push(Kiq.load_json(member))
          end
        end
      end
    end

    class Poller
      INITIAL_WAIT = 10.0

      getter config : Config

      def initialize(@config : Config)
        @enq = Enq.new(@config)
        @done = false
      end

      def terminate : Nil
        @done = true
        @enq.terminate
      end

      def start : Fiber
        spawn(name: "kiq/scheduler") do
          initial_wait
          until @done
            enqueue
            wait
          end
        end
      end

      def enqueue : Nil
        @enq.enqueue_jobs
      rescue ex : Exception
        Kiq.handle_exception(ex)
      end

      private def initial_wait : Nil
        total = 0.0
        total += INITIAL_WAIT unless @config.poll_interval_average
        total += 5 * rand
        sleep(total.seconds) unless total.zero?
      end

      private def wait : Nil
        interval = random_poll_interval
        sleep(interval.seconds) unless interval.zero?
      end

      # 1:1 with Poller#random_poll_interval (scaled by process count;
      # we do not register in the `processes` set in P0, so count is 1).
      private def random_poll_interval : Float64
        count = process_count
        interval = @config.poll_interval_average || scaled_poll_interval(count)
        if count < 10
          interval * rand + interval / 2
        else
          interval * rand * 2
        end
      end

      private def scaled_poll_interval(process_count : Int64) : Float64
        process_count * @config.average_scheduled_poll_interval
      end

      private def process_count : Int64
        pcount = @config.redis_client.scard("processes")
        pcount.zero? ? 1_i64 : pcount
      end
    end
  end
end

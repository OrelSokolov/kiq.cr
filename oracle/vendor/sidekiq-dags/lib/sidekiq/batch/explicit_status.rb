module Sidekiq
  class Batch

    class Status
      def exists?
        case Sidekiq.redis { |r| r.exists("BID-#{bid}") }
        when 1
          true
        else
          false
        end
      end
    end

    class ExplicitStatus
      attr_reader :bid

      def initialize(bid)
        @bid = bid
      end

      def join
        raise "Not supported"
      end

      def exists?
        case Sidekiq.redis { |r| r.exists("BID-#{bid}") }
        when 1
          true
        else
          false
        end
      end

      def pending
        Sidekiq.redis { |r| r.hget("BID-#{bid}", 'pending') }
      end

      def failures
        Sidekiq.redis { |r| r.scard("BID-#{bid}-failed") }
      end

      def created_at
        Sidekiq.redis { |r| r.hget("BID-#{bid}", 'created_at') }
      end

      def total
        Sidekiq.redis { |r| r.hget("BID-#{bid}", 'total') }
      end

      def parent_bid
        Sidekiq.redis { |r| r.hget("BID-#{bid}", "parent_bid") }
      end

      def failure_info
        Sidekiq.redis { |r| r.smembers("BID-#{bid}-failed") }
      end

      def complete?
        var = Sidekiq.redis { |r| r.hget("BID-#{bid}", 'complete') }
        case var
        when 'true'
          true
        when 'false'
          false
        else
          nil
        end
      end

      def child_count
        Sidekiq.redis { |r| r.hget("BID-#{bid}", 'children') }
      end

      def data
        {
          bid: bid,
          total: total,
          failures: failures,
          pending: pending,
          created_at: created_at,
          complete: complete?,
          failure_info: failure_info,
          parent_bid: parent_bid,
          child_count: child_count
        }
      end
    end
  end
end

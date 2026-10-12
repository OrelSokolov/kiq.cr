# Status snapshot, it is used after the end of job

require 'base64'

module Sidekiq
  class Batch
    class FinalStatusSnapshot
      attr_reader :bid,
                  :total,
                  :failures,
                  :pending,
                  :created_at,
                  :complete,
                  :failure_info,
                  :parent_bid,
                  :child_count

      def initialize(bid)
        status = Sidekiq::Batch::Status.new(bid)

        @bid         = status.bid
        @total       = status.total
        @failures    = status.failures
        @pending     = status.pending
        @created_at  = status.created_at
        @complete    = status.complete?
        @failure_info = status.failure_info
        @parent_bid  = status.parent_bid
        @child_count = status.child_count
      end

      def complete?
        @complete
      end

      def success?
        @success
      end

      def serialized
        Base64.encode64(Marshal.dump(self))
      end

      def self.deserialize(serialized_data)
        Marshal.load(Base64.decode64(serialized_data))
      end

      def data
        to_h
      end

      def exists?
        true
      end

      def to_h
        {
          bid: @bid,
          total: @total,
          failures: @failures,
          pending: @pending,
          created_at: @created_at,
          complete: @complete,
          failure_info: @failures_info,
          parent_bit: @parent_bid,
          child_count: @child_count
        }
      end
    end
  end
end

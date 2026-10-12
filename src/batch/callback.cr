# Kiq::Batch::Callback — callback dispatch (sidekiq-dags
# lib/sidekiq/batch/callback.rb + extension/worker.rb).
#
# Callback classes include Kiq::Batch::CallbackHandler and define
# `on_complete(status, options)` / `on_success(status, options)`. They are
# auto-registered into a compile-time registry at inclusion (Crystal has no
# runtime const-get; documented deviation).

module Kiq
  class Batch
    module Extension
      # Included into job classes: access to the batch context the job is
      # executing within (1:1 with sidekiq-dags Worker extension).
      module Worker
        def bid : String?
          Batch.current.try(&.bid)
        end

        def batch : Batch?
          Batch.current
        end

        def valid_within_batch? : Bool
          !!batch.try(&.valid?)
        end
      end
    end

    module Callback
      # Instances receive (status, options) for the events they define.
      # `status` is a Kiq::Batch::FinalStatusSnapshot.
      module CallbackHandler
        macro included
          Kiq::Batch::Callback::Registry.register({{ @type.stringify }}, {{ @type.id }}.new)
        end
      end

      class Registry
        @@handlers = {} of String => CallbackHandler

        def self.register(name : String, handler : CallbackHandler) : Nil
          # last registration wins, like const_get finding the newest
          # constant — and allows spec redefinition
          @@handlers[name] = handler
        end

        def self.[](name : String) : CallbackHandler?
          @@handlers[name]?
        end

        def self.[]?(name : String) : CallbackHandler?
          @@handlers[name]?
        end

        def self.clear : Nil
          @@handlers.clear
        end
      end

      # The callback job: performs a registered handler's on_<event>.
      # Args: [clazz, event, opts, bid, parent_bid, serialized_status].
      class Worker
        include Kiq::Job
        include Kiq::Batch::Extension::Worker

        def perform(clazz : Arg, event : Arg, opts : Arg, bid : Arg,
                    parent_bid : Arg, serialized_status : Arg)
          return unless {"success", "complete"}.includes?(event.to_s)
          clazz = clazz.to_s
          method_name = "on_#{event}"
          if clazz.includes?("#")
            clazz, method_name = clazz.split("#")
          end
          status = FinalStatusSnapshot.deserialize(serialized_status.to_s)

          if (handler = Registry[clazz]?)
            case method_name
            when "dispatch" then handler.as?(Finalize).try(&.dispatch(status, opts_hash(opts)))
            when "on_complete" then handler.as?(OnComplete).try(&.on_complete(status, opts_hash(opts)))
            when "on_success" then handler.as?(OnSuccess).try(&.on_success(status, opts_hash(opts)))
            end
          end
        end

        private def opts_hash(opts : Arg) : Hash(String, Arg)
          case opts
          when Hash(String, Arg) then opts
          else {} of String => Arg
          end
        end
      end

      # Optional event interfaces (a handler may define either or both).
      module OnComplete
        abstract def on_complete(status : Kiq::Batch::FinalStatusSnapshot, options : Hash(String, Arg)) : Nil
      end

      module OnSuccess
        abstract def on_success(status : Kiq::Batch::FinalStatusSnapshot, options : Hash(String, Arg)) : Nil
      end

      # Finalize: parent-batch bookkeeping after all callbacks ran (1:1).
      class Finalize
        include CallbackHandler
        include OnComplete

        def on_complete(status, options) : Nil
          dispatch(status, options)
        end

        def dispatch(status, options) : Nil
          bid = options["bid"]?.try(&.to_s).to_s
          callback_bid = status.bid
          event = (options["event"]?.try(&.to_s) || "complete")

          batch_status = Status.new bid
          case event
          when "success"
            success(bid, batch_status, batch_status.parent_bid)
          when "complete"
            complete(bid, batch_status, batch_status.parent_bid)
          end
        end

        def success(bid : String, status : Status, parent_bid : String?) : Nil
          return unless parent_bid

          results = Kiq.default_configuration.redis_client.multi do |tx|
            tx.sadd("BID-#{parent_bid}-success", [bid])
            tx.expire("BID-#{parent_bid}-success", BID_EXPIRE_TTL)
            tx.scard("BID-#{parent_bid}-success")
            tx.sadd("BID-#{parent_bid}-complete", [bid])
            tx.scard("BID-#{parent_bid}-complete")
            tx.hincrby("BID-#{parent_bid}", "pending", 0)
            tx.hincrby("BID-#{parent_bid}", "children", 0)
            tx.scard("BID-#{parent_bid}-failed")
          end
          r = results.not_nil!
          success_count = r[2].to_s.to_i64
          complete = r[4].to_s.to_i64
          pending = r[5].to_s.to_i64
          children = r[6].to_s.to_i64
          failure = r[7].to_s.to_i64

          # if job finished successfully and parent batch completed call
          # parent complete callback
          if complete == children && pending == failure
            Batch.enqueue_callbacks(:complete, parent_bid)
          end
        end

        def complete(bid : String, status : Status, parent_bid : String?) : Nil
          redis = Kiq.default_configuration.redis_client
          results = redis.multi do |tx|
            tx.hincrby("BID-#{bid}", "pending", 0)
            tx.hincrby("BID-#{bid}", "children", 0)
            tx.scard("BID-#{bid}-success")
          end
          r = results.not_nil!
          pending = r[0].to_s.to_i64
          children = r[1].to_s.to_i64
          success = r[2].to_s.to_i64

          # if the batch was successful run success callback
          if pending.zero? && children == success
            Batch.enqueue_callbacks(:success, bid)
          elsif parent_bid
            # if batch was not successfull check and see if its parent is
            # complete; if so trigger the parent complete callback
            results2 = redis.multi do |tx|
              tx.sadd("BID-#{parent_bid}-complete", [bid])
              tx.scard("BID-#{parent_bid}-complete")
              tx.hincrby("BID-#{parent_bid}", "pending", 0)
              tx.hincrby("BID-#{parent_bid}", "children", 0)
              tx.scard("BID-#{parent_bid}-failed")
            end
            r2 = results2.not_nil!
            complete = r2[1].to_s.to_i64
            pending2 = r2[2].to_s.to_i64
            children2 = r2[3].to_s.to_i64
            failure2 = r2[4].to_s.to_i64
            if complete == children2 && pending2 == failure2
              Batch.enqueue_callbacks(:complete, parent_bid)
            end
          end
        end
      end
    end
  end
end

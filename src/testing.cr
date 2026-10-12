# Kiq::Testing — fake!/inline!/disable! modes, 1:1 with sidekiq's
# test_api (Sidekiq.testing! / Sidekiq::Testing / Sidekiq::Queues).
#
# Deviation: the test mode is process-global (Ruby has a thread-local
# override); Crystal specs run fibers on one thread, so the block form
# (save/restore) covers the use cases.

module Kiq
  class Testing
    @@global_test_mode : String? = nil

    def self.__set_test_mode(mode : String, &block : -> _)
      if block
        previous = @@global_test_mode
        @@global_test_mode = mode
        begin
          block.call
        ensure
          @@global_test_mode = previous
        end
      else
        @@global_test_mode = mode
      end
    end

    def self.__test_mode : String?
      @@global_test_mode
    end

    def self.disable!(&block : -> _?)
      __set_test_mode("disable", &block)
    end

    def self.fake!(&block : -> _?)
      __set_test_mode("fake", &block)
    end

    def self.inline!(&block : -> _?)
      __set_test_mode("inline", &block)
    end

    def self.enabled? : Bool
      __test_mode != "disable"
    end

    def self.disabled? : Bool
      __test_mode == "disable"
    end

    def self.fake? : Bool
      __test_mode == "fake"
    end

    def self.inline? : Bool
      __test_mode == "inline"
    end
  end

  # The fake queue implementation: jobs by queue and by class, 1:1 with
  # Sidekiq::Queues.
  module Queues
    def self.[](queue : String) : Array(Msg)
      jobs_by_queue[queue] ||= [] of Msg
    end

    def self.push(queue : String, klass : String, job : Msg) : Nil
      (jobs_by_queue[queue] ||= [] of Msg) << job
      (jobs_by_class[klass] ||= [] of Msg) << job
    end

    def self.jobs_by_queue : Hash(String, Array(Msg))
      @@jobs_by_queue ||= {} of String => Array(Msg)
    end

    def self.jobs_by_class : Hash(String, Array(Msg))
      @@jobs_by_class ||= {} of String => Array(Msg)
    end

    def self.jobs_for(klass : String) : Array(Msg)
      jobs_by_class[klass] ||= [] of Msg
    end

    def self.delete_for(job : Msg) : Nil
      jid = job["jid"]?.try(&.to_s)
      return unless jid
      jobs_by_queue.each_value { |jobs| jobs.reject! { |j| j["jid"]?.try(&.to_s) == jid } }
      jobs_by_class.each_value { |jobs| jobs.reject! { |j| j["jid"]?.try(&.to_s) == jid } }
    end

    def self.clear_for(klass : String) : Nil
      jobs_by_class.delete(klass)
    end

    def self.clear_all : Nil
      jobs_by_queue.clear
      jobs_by_class.clear
    end
  end
end

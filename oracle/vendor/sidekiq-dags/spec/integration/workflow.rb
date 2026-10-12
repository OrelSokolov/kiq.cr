require 'integration_helper'

# Complex workflow with sequential and nested
# Also test sub batches without callbacks
# Batches:
# - Overall
#  - Worker1
#   - Worker3
#  - Worker2 + Worker3
#   - Worker1
#    - Worker3
#  - Worker4
#  - Worker5

class Callbacks
  def worker1 status, opts
    Sidekiq.logger.info "Success 1 #{status.data}"

    overall = Sidekiq::Batch.new status.parent_bid
    overall.add_jobs do
      batch = Sidekiq::Batch.new
      batch.on(:success, "Callbacks#worker2")
      batch.add_jobs do
        Worker2.perform_async
      end
      batch.run
    end
    overall.run
  end

  def worker2 status, opts
    Sidekiq.logger.info "Success 2 #{status.data}"
    overall = Sidekiq::Batch.new status.parent_bid
    overall.add_jobs do
      batch = Sidekiq::Batch.new
      batch.on(:success, "Callbacks#worker4")
      batch.add_jobs do
        Worker4.perform_async
      end
      batch.run
    end
    overall.run

  end

  def worker4 status, opts
    Sidekiq.logger.info "Success 4 #{status.data}"
    overall = Sidekiq::Batch.new status.parent_bid
    overall.add_jobs do
      batch = Sidekiq::Batch.new
      batch.on(:success, "Callbacks#worker5")
      batch.add_jobs do
        Worker5.perform_async
      end
      batch.run
    end
    overall.run
  end

  def worker5 status, opts
    Sidekiq.logger.info "Success 5 #{status.data}"
  end
end

class Worker1
  include Sidekiq::Worker

  def perform
    Sidekiq.logger.info "Work 1"
    batch = Sidekiq::Batch.new
    batch.add_jobs do
      Worker3.perform_async
    end
    batch.run
  end
end

class Worker2
  include Sidekiq::Worker

  def perform
    Sidekiq.logger.info "Work 2"
    if bid
      batch.add_jobs do
        Worker3.perform_async
      end
      batch.run
      newb = Sidekiq::Batch.new
      newb.add_jobs do
        Worker1.perform_async
      end
      newb.run
    end
  end
end

class Worker3
  include Sidekiq::Worker
  def perform
    Sidekiq.logger.info "Work 3"
  end
end

class Worker4
  include Sidekiq::Worker
  def perform
    Sidekiq.logger.info "Work 4"
  end
end

class Worker5
  include Sidekiq::Worker
  def perform
    Sidekiq.logger.info "Work 5"
  end
end

class MyCallback
  def on_success(status, options)
    Sidekiq.logger.info "Overall Success #{options} #{status.data}"
  end
  alias_method :multi, :on_success

  def on_complete(status, options)
    Sidekiq.logger.info "Overall Complete #{options} #{status.data}"
  end
end

overall = Sidekiq::Batch.new
overall.on(:success, MyCallback, to: 'success@gmail.com')
overall.on(:complete, MyCallback, to: 'success@gmail.com')
overall.add_jobs do
  batch1 = Sidekiq::Batch.new
  batch1.on(:success, "Callbacks#worker1")
  batch1.add_jobs do
    Worker1.perform_async
  end
  batch1.run
end
overall.run

puts "Overall bid #{overall.bid}"

output, keys = process_tests
overall_tests output, keys

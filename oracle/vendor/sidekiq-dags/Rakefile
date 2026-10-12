require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

task :default => :spec

# Задача для тестирования batch coordination
namespace :test do
  desc "Run batch coordination tests with detailed output"
  task :coordination do
    require 'rspec/core'
    
    options = RSpec::Core::ConfigurationOptions.new([
      'spec/integration/batch_coordination_spec.rb',
      '--format', 'documentation',
      '--color'
    ])
    
    runner = RSpec::Core::Runner.new(options)
    exit runner.run($stderr, $stdout)
  end
  
  desc "Manual test of batch coordination with rate limiter"
  task :coordination_manual do
    require 'sidekiq'
    require 'sidekiq/testing'
    require_relative 'lib/sidekiq_dags'
    
    puts "🧪 Batch Coordination Manual Test"
    puts "=" * 50
    
    # Настройка Sidekiq
    Sidekiq.configure_client do |config|
      config.redis = { url: ENV.fetch('REDIS_URL', 'redis://localhost:6379/15') }
    end
    
    Sidekiq.configure_server do |config|
      config.redis = { url: ENV.fetch('REDIS_URL', 'redis://localhost:6379/15') }
    end
    
    # Включаем inline mode
    Sidekiq::Testing.inline!
    
    # Очищаем Redis
    puts "🧹 Cleaning Redis..."
    Sidekiq.redis(&:flushdb)
    
    # Rate Limiter Middleware
    class TestRateLimiterMiddleware
      def initialize(delay_ms = 300)
        @delay_ms = delay_ms
      end
      
      def call(worker_class, job, queue, redis_pool)
        worker_name = worker_class.is_a?(String) ? worker_class : worker_class.name
        
        if worker_name.include?('RateLimitedJob')
          puts "⏱️  [#{Time.now.strftime('%H:%M:%S.%3N')}] Rate limiter: delaying by #{@delay_ms}ms"
          sleep(@delay_ms / 1000.0)
        end
        
        yield
      end
    end
    
    # Test Job
    class RateLimitedJob
      include Sidekiq::Job
      
      def perform(value)
        puts "✅ [#{Time.now.strftime('%H:%M:%S.%3N')}] RateLimitedJob executed: #{value}"
      end
    end
    
    # Test Node
    class TestNode < Sidekiq::Node
      desc "Test node with rate limited job"
      
      execute do |*args|
        value = args.first || 42
        puts "🔧 [#{Time.now.strftime('%H:%M:%S.%3N')}] Creating RateLimitedJob..."
        RateLimitedJob.perform_async(value)
      end
    end
    
    # Регистрируем middleware
    delay = ENV['DELAY_MS']&.to_i || 300
    puts "📝 Registering rate limiter (#{delay}ms delay)..."
    Sidekiq.configure_client do |config|
      config.client_middleware do |chain|
        chain.add TestRateLimiterMiddleware, delay
      end
    end
    
    # Включаем debug логи
    Sidekiq.logger = Logger.new(STDOUT)
    Sidekiq.logger.level = Logger::DEBUG
    
    # Запускаем тест
    puts "\n" + "=" * 50
    puts "🚀 Starting test..."
    puts "=" * 50
    
    start_time = Time.now
    TestNode.perform_async(42)
    duration = Time.now - start_time
    
    puts "\n" + "=" * 50
    puts "✅ Test completed in #{duration.round(3)}s"
    puts "=" * 50
    puts "\nExpected: ~#{(delay + 200) / 1000.0}s (#{delay}ms delay + 200ms stability)"
    puts "Actual: #{duration.round(3)}s"
    
    if duration >= (delay + 200) / 1000.0 - 0.1
      puts "\n🎉 SUCCESS: Coordination working correctly!"
    else
      puts "\n⚠️  WARNING: Duration too short, coordination may not be working"
    end
  end
end

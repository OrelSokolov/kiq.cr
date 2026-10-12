require "simplecov"
SimpleCov.start

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

# Загружаем ActiveRecord и настраиваем БД перед загрузкой sidekiq/batch
begin
  require 'active_record'
  require 'active_support/core_ext'
rescue LoadError
  # ActiveRecord не доступен, пропускаем
end

# Загружаем настройку БД перед загрузкой sidekiq/batch
Dir[File.dirname(__FILE__) + "/support/**/*.rb"].each {|f| require f }

require 'sidekiq/batch'

redis_opts = { url: "redis://127.0.0.1:6379/1" }

Sidekiq.configure_client do |config|
  config.redis = redis_opts
end

Sidekiq.configure_server do |config|
  config.redis = redis_opts
end

RSpec.configure do |config|
  config.filter_run focus: true
  config.run_all_when_everything_filtered = true

  config.before do
    Sidekiq.redis do |redis|
      redis.flushdb
    end
  end
end

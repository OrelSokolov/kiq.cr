# frozen_string_literal: true

if defined?(Rails::Railtie)
  module Sidekiq
    module Pipeline
      # Railtie для автоматической загрузки rake-задач в Rails приложении
      class Railtie < Rails::Railtie
        rake_tasks do
          load 'tasks/sidekiq_dags.rake'
        end
      end
    end
  end
end


# frozen_string_literal: true

# DummyJob используется для создания непустого батча в Node
# Это необходимо для корректной работы batch callbacks
class DummyJob
  include Sidekiq::Job
  
  def perform(desc = 'dummy')
    # Пустая джоба для координации batch
    # desc - описание ноды для логирования
  end
end


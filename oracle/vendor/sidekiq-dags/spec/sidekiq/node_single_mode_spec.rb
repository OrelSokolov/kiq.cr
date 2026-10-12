# frozen_string_literal: true

require_relative '../spec_helper'
require 'sidekiq/testing'

Sidekiq::Testing.server_middleware do |chain|
  chain.add Sidekiq::Batch::Middleware::ServerMiddleware
end

# Добавляем метод present? для совместимости с Node
class String
  def present?
    !empty?
  end
end

class NilClass
  def present?
    false
  end
end

class Class
  def present?
    true
  end
end

# DummyJob для тестов
class DummyJob
  include Sidekiq::Worker
  def perform(msg = 'dummy'); end
end

# Тестовый пайплайн: Alpha
module Alpha
  extend Sidekiq::NodeDsl
  
  class AlphaBaseNode < Sidekiq::Node
    include Sidekiq::PipelineTracking
    sidekiq_options queue: 'alpha'
  end
  
  node :RootNode, AlphaBaseNode do
    desc "Alpha RootNode"
    next_node nil
    execute { }
  end
end

# Тесты для single mode в Sidekiq::Node
# Проверяем что Node#perform правильно извлекает single из args (после JSON сериализации)
RSpec.describe Sidekiq::Node do
  before(:each) do
    Sidekiq::Testing.fake!
    Sidekiq::Job.clear_all
    
    # Сбрасываем состояние пайплайна
    if defined?(Sidekiq::SidekiqPipeline)
      Sidekiq::SidekiqPipeline.destroy_all
    end
  end

  after(:each) do
    Sidekiq::Job.clear_all
  end

  describe '#perform single mode extraction' do
    let(:mock_batch) { instance_double(Sidekiq::Batch, bid: 'test_bid') }

    before do
      allow(Sidekiq::Batch).to receive(:new).and_return(mock_batch)
      allow(mock_batch).to receive(:add_jobs).and_yield
      allow(mock_batch).to receive(:on)
      allow(mock_batch).to receive(:run)
      allow(Sidekiq::Batch::ExplicitStatus).to receive(:new).and_return(
        instance_double(Sidekiq::Batch::ExplicitStatus, total: 1, exists?: true)
      )
    end

    context 'when single mode is passed as hash in args (simulating JSON serialization)' do
      let(:node) { Alpha::RootNode.new }
      
      before do
        allow(node).to receive(:save_bid_to_database!)
        allow(node).to receive(:notify_all)
      end
      
      it 'passes single=true to batch callback options' do
        # Симулируем как Sidekiq передает аргументы после JSON сериализации
        args = [{ 'single' => true }]
        kwargs = {}

        node.perform(*args, **kwargs)

        # Проверяем что batch.on был вызван с single => true
        expect(mock_batch).to have_received(:on).with(
          :complete,
          Sidekiq::PipelineCallback,
          hash_including('single' => true)
        )
      end

      it 'removes options hash from args before calling execute' do
        args = [{ 'single' => true, 'other' => 'value' }]
        kwargs = {}

        # Проверяем что execute вызывается без опций
        expect(node).to receive(:execute).with(no_args)

        node.perform(*args, **kwargs)
      end
    end

    context 'when single mode is passed as kwargs (direct call)' do
      let(:node) { Alpha::RootNode.new }
      
      before do
        allow(node).to receive(:save_bid_to_database!)
        allow(node).to receive(:notify_all)
      end
      
      it 'passes single=true to batch callback options' do
        args = []
        kwargs = { single: true }

        node.perform(*args, **kwargs)

        expect(mock_batch).to have_received(:on).with(
          :complete,
          Sidekiq::PipelineCallback,
          hash_including('single' => true)
        )
      end
    end

    context 'when single mode is passed as symbol key in args' do
      let(:node) { Alpha::RootNode.new }
      
      before do
        allow(node).to receive(:save_bid_to_database!)
        allow(node).to receive(:notify_all)
      end
      
      it 'passes single=true to batch callback options' do
        args = [{ single: true }]  # Symbol key
        kwargs = {}

        node.perform(*args, **kwargs)

        expect(mock_batch).to have_received(:on).with(
          :complete,
          Sidekiq::PipelineCallback,
          hash_including('single' => true)
        )
      end
    end

    context 'when single mode is not passed' do
      let(:node) { Alpha::RootNode.new }
      
      before do
        allow(node).to receive(:save_bid_to_database!)
        allow(node).to receive(:notify_all)
      end
      
      it 'defaults to false' do
        args = []
        kwargs = {}

        node.perform(*args, **kwargs)

        expect(mock_batch).to have_received(:on).with(
          :complete,
          Sidekiq::PipelineCallback,
          hash_including('single' => false)
        )
      end
    end

    context 'when args contain hash but without single key' do
      let(:node) { Alpha::RootNode.new }
      
      before do
        allow(node).to receive(:save_bid_to_database!)
        allow(node).to receive(:notify_all)
      end
      
      it 'does not treat hash as single mode options' do
        args = [{ 'some_param' => 'value', 'another' => 123 }]
        kwargs = {}

        node.perform(*args, **kwargs)

        # single_mode должен быть false
        expect(mock_batch).to have_received(:on).with(
          :complete,
          Sidekiq::PipelineCallback,
          hash_including('single' => false)
        )
      end
    end
  end
end

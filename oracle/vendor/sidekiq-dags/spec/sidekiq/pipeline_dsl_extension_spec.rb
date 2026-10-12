# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/batch'

describe Sidekiq::PipelineDslExtension do
  before(:each) do
    Sidekiq::PipelineDslExtension.clear_registry
  end

  describe '.node_registry' do
    it 'returns empty hash initially' do
      expect(Sidekiq::PipelineDslExtension.node_registry).to eq({})
    end
  end

  describe '.register_node' do
    it 'registers node metadata' do
      metadata = {
        description: 'Test node',
        next_node: 'NextNode',
        jobs: ['TestJob'],
        has_observer: true,
        has_execute: true
      }
      
      Sidekiq::PipelineDslExtension.register_node('TestModule', 'TestNode', metadata)
      
      registry = Sidekiq::PipelineDslExtension.node_registry
      expect(registry['TestModule::TestNode']).to eq(metadata)
    end
  end

  describe '.get_pipeline_data' do
    it 'returns nodes for specific module' do
      metadata1 = { description: 'Node1', next_node: 'Node2', jobs: [], has_observer: false, has_execute: false }
      metadata2 = { description: 'Node2', next_node: nil, jobs: [], has_observer: false, has_execute: false }
      metadata3 = { description: 'OtherNode', next_node: nil, jobs: [], has_observer: false, has_execute: false }
      
      Sidekiq::PipelineDslExtension.register_node('TestModule', 'Node1', metadata1)
      Sidekiq::PipelineDslExtension.register_node('TestModule', 'Node2', metadata2)
      Sidekiq::PipelineDslExtension.register_node('OtherModule', 'OtherNode', metadata3)
      
      pipeline_data = Sidekiq::PipelineDslExtension.get_pipeline_data('TestModule')
      
      expect(pipeline_data.keys).to contain_exactly('Node1', 'Node2')
      expect(pipeline_data['Node1']).to eq(metadata1)
      expect(pipeline_data['Node2']).to eq(metadata2)
    end
  end

  describe 'NodeDsl integration' do
    it 'registers node metadata when node is created' do
      module TestModule
        extend Sidekiq::NodeDsl
        
        node :TestNode do
          desc "Test description"
          execute do
            TestJob.perform_async
          end
          next_node :NextNode
        end
      end
      
      registry = Sidekiq::PipelineDslExtension.node_registry
      expect(registry).to have_key('TestModule::TestNode')
      
      metadata = registry['TestModule::TestNode']
      expect(metadata[:description]).to eq('Test description')
      expect(metadata[:next_node]).to eq('NextNode')
      expect(metadata[:jobs]).to include('TestJob')
      expect(metadata[:has_execute]).to be true
    end
  end
end


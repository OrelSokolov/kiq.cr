# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/batch'

describe Sidekiq::PipelineVisualizer do
  describe 'Node' do
    let(:node) { Sidekiq::PipelineVisualizer::Node.new('TestNode', 'Test description', 'NextNode', 'TestModule', ['TestJob']) }

    it 'initializes with correct attributes' do
      expect(node.name).to eq('TestNode')
      expect(node.description).to eq('Test description')
      expect(node.next_node).to eq('NextNode')
      expect(node.module_name).to eq('TestModule')
      expect(node.jobs).to eq(['TestJob'])
    end

    it 'generates correct id' do
      expect(node.id).to eq('TestModule_TestNode')
    end

    it 'calculates dimensions' do
      node.calculate_dimensions
      expect(node.width).to be >= 200
      expect(node.height).to be >= 70
    end
  end

  describe 'Pipeline' do
    let(:pipeline) { Sidekiq::PipelineVisualizer::Pipeline.new('TestPipeline') }

    it 'initializes with correct name' do
      expect(pipeline.name).to eq('TestPipeline')
      expect(pipeline.nodes).to eq({})
    end

    it 'adds nodes' do
      pipeline.add_node('Node1', 'Description1', 'Node2', 'TestModule', ['Job1'])
      expect(pipeline.nodes).to have_key('Node1')
      expect(pipeline.nodes['Node1'].name).to eq('Node1')
      expect(pipeline.nodes['Node1'].description).to eq('Description1')
    end

    it 'finds root node' do
      pipeline.add_node('RootNode', 'Root', 'Node2', 'TestModule')
      pipeline.add_node('Node2', 'Node2', nil, 'TestModule')

      root = pipeline.get_root_node
      expect(root).not_to be_nil
      expect(root.name).to eq('RootNode')
    end

    it 'calculates layout' do
      pipeline.add_node('RootNode', 'Root', 'Node2', 'TestModule')
      pipeline.add_node('Node2', 'Node2', nil, 'TestModule')

      pipeline.calculate_layout

      root = pipeline.nodes['RootNode']
      node2 = pipeline.nodes['Node2']

      expect(root.x).to be >= 0
      expect(root.y).to be >= 0
      expect(node2.x).to be > root.x
    end

    it 'generates SVG' do
      pipeline.add_node('RootNode', 'Root', 'Node2', 'TestModule', ['TestJob'])
      pipeline.add_node('Node2', 'Node2', nil, 'TestModule')

      svg = pipeline.to_svg

      expect(svg).to include('<svg')
      expect(svg).to include('RootNode')
      expect(svg).to include('Node2')
      expect(svg).to include('TestJob')
    end

    it 'generates SVG with statuses' do
      pipeline.add_node('RootNode', 'Root', 'Node2', 'TestModule')
      pipeline.add_node('Node2', 'Node2', nil, 'TestModule')

      node_statuses = { 'TestModule_RootNode' => 'running', 'TestModule_Node2' => 'completed' }
      svg = pipeline.to_svg(node_statuses: node_statuses, pipeline_running: true)

      expect(svg).to include('node-rect-running')
      expect(svg).to include('node-rect-completed')
    end
  end

  describe 'Collector' do
    before(:each) do
      Sidekiq::PipelineDslExtension.clear_registry
    end

    it 'collects pipelines from registered nodes' do
      # Регистрируем узлы
      metadata1 = {
        description: 'Root node',
        next_node: 'Node2',
        jobs: ['Job1'],
        has_observer: false,
        has_execute: true
      }
      metadata2 = {
        description: 'Node 2',
        next_node: nil,
        jobs: ['Job2'],
        has_observer: false,
        has_execute: true
      }

      Sidekiq::PipelineDslExtension.register_node('TestModule', 'RootNode', metadata1)
      Sidekiq::PipelineDslExtension.register_node('TestModule', 'Node2', metadata2)

      pipelines = Sidekiq::PipelineVisualizer::Collector.collect_pipelines

      expect(pipelines).to have_key('test_module')
      pipeline = pipelines['test_module']
      expect(pipeline).to be_a(Sidekiq::PipelineVisualizer::Pipeline)
      expect(pipeline.nodes).to have_key('RootNode')
      expect(pipeline.nodes).to have_key('Node2')
    end

    it 'handles multiple pipelines' do
      Sidekiq::PipelineDslExtension.register_node('Module1', 'RootNode', {
                                                    description: 'Root1',
                                                    next_node: nil,
                                                    jobs: [],
                                                    has_observer: false,
                                                    has_execute: false
                                                  })
      Sidekiq::PipelineDslExtension.register_node('Module2', 'RootNode', {
                                                    description: 'Root2',
                                                    next_node: nil,
                                                    jobs: [],
                                                    has_observer: false,
                                                    has_execute: false
                                                  })

      pipelines = Sidekiq::PipelineVisualizer::Collector.collect_pipelines

      expect(pipelines).to have_key('module1')
      expect(pipelines).to have_key('module2')
    end
  end
end

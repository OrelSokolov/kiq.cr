# frozen_string_literal: true

# Настройка базы данных для тестов
begin
  require 'active_record'
  
  # Используем SQLite в памяти для тестов
  ActiveRecord::Base.establish_connection(
    adapter: 'sqlite3',
    database: ':memory:'
  )
  
  # Создаем таблицы для тестов
  ActiveRecord::Schema.define do
    create_table :sidekiq_pipelines, force: true do |t|
      t.string :pipeline_name, null: false
      t.integer :status, default: 0
      t.datetime :run_at
      t.timestamps
    end
    
    add_index :sidekiq_pipelines, :pipeline_name, unique: true
    add_index :sidekiq_pipelines, :status
    
    create_table :sidekiq_pipeline_nodes, force: true do |t|
      t.references :sidekiq_pipeline, null: false, foreign_key: false
      t.string :node_name, null: false
      t.integer :status, default: 0, null: false
      t.datetime :run_at
      t.text :error_message
      t.timestamps
    end
    
    add_index :sidekiq_pipeline_nodes, [:sidekiq_pipeline_id, :node_name], unique: true, name: 'index_sidekiq_pipeline_nodes_on_pipeline_and_node'
    add_index :sidekiq_pipeline_nodes, :sidekiq_pipeline_id
    add_index :sidekiq_pipeline_nodes, :status
  end
  
  puts "Database initialized for tests" if ENV['DEBUG']
rescue LoadError => e
  puts "ActiveRecord not available: #{e.message}" if ENV['DEBUG']
rescue => e
  puts "Database setup error: #{e.message}" if ENV['DEBUG']
end


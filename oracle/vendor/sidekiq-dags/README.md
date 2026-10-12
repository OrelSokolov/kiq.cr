[gem]: https://rubygems.org/gems/sidekiq-dags
[ci]: https://github.com/breamware/sidekiq-batch/actions/workflows/ci.yml

# Sidekiq DAGs

Simple Sidekiq DAGs implementation. Based on `sidekiq-batch` gem 

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sidekiq-dags', github: 'OrelSokolov/sidekiq-dags'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sidekiq-dags

## Usage

Sidekiq Batch is MOSTLY a drop-in replacement for the API from Sidekiq PRO. See https://github.com/mperham/sidekiq/wiki/Batches for usage.

### Pipeline DSL

Новый DSL для создания пайплайнов напрямую из батчей:

```ruby
require 'sidekiq_dags'

module MyApp
  extend Sidekiq::NodeDsl

  pipeline :data_processing do
    node :start_node do
      desc "Start processing"
      execute do
        StartJob.perform_async
      end
      next_node :process_node
    end

    node :process_node do
      desc "Process data"
      execute do
        ProcessJob.perform_async
      end
      next_node :end_node
    end

    node :end_node do
      desc "Finish"
      execute do
        FinishJob.perform_async
      end
    end
  end
end

# Запуск пайплайна
MyApp::DataProcessing::StartNode.perform_async
```

### Pipeline Status Tracking

Пайплайны автоматически отслеживают статусы в базе данных через модели `SidekiqPipeline` и `SidekiqPipelineNode`:

```ruby
# Получить пайплайн
pipeline = Sidekiq::SidekiqPipeline.for('myapp')

# Проверить статус
pipeline.running?   # => true/false
pipeline.completed? # => true/false
pipeline.failed?    # => true/false
pipeline.idle?      # => true/false

# Получить прогресс
pipeline.progress_percent # => 75.5

# Получить текущую ноду
pipeline.current_node # => SidekiqPipelineNode

# Получить завершённые ноды
pipeline.completed_nodes # => ActiveRecord::Relation
```

### Pipeline Metadata & Visualization

Гем автоматически регистрирует метаданные узлов при их создании через DSL:

```ruby
# Получить метаданные всех узлов модуля
metadata = Sidekiq::PipelineDslExtension.get_pipeline_data('MyApp')
# => { "StartNode" => { description: "...", next_node: "...", jobs: [...], ... }, ... }

# Очистить реестр (для тестов)
Sidekiq::PipelineDslExtension.clear_registry
```

Визуализация пайплайнов в виде SVG:

```ruby
# Собрать все пайплайны из зарегистрированных узлов
pipelines = Sidekiq::PipelineVisualizer::Collector.collect_pipelines

# Получить SVG для конкретного пайплайна
pipeline = pipelines['myapp']
svg = pipeline.to_svg(
  active_nodes: Set.new(['myapp_StartNode']),
  pipeline_running: true,
  node_statuses: { 'myapp_StartNode' => 'running', 'myapp_EndNode' => 'completed' }
)
```

### Database Setup

Для работы с отслеживанием статусов необходимо скопировать миграции в ваше Rails приложение:

```bash
rake sidekiq_dags:install
```

Или скопировать вручную миграции из `db/migrate/` в `db/migrate/` вашего приложения, затем выполнить:

```bash
rails db:migrate
```

Миграции создадут таблицы:
- `sidekiq_pipelines` - для хранения состояния пайплайнов
- `sidekiq_pipeline_nodes` - для хранения состояния нод пайплайна

## License

This gem is based on `sidekiq-batch` gem

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

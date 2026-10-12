# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sidekiq/batch/version'

Gem::Specification.new do |spec|
  spec.name          = "sidekiq-dags"
  spec.version       = Sidekiq::Batch::VERSION
  spec.authors       = ["Oleg Orlov"]
  spec.email         = ["orelcokolov@gmail.com"]

  spec.summary       = "DAGs for Sidekiq"
  spec.description   = "Sidekiq DAGs implementation base on open-source Sidekiq::Batch"
  spec.homepage      = "http://github.com/orelsokolov/sidekiq-dags"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.files         += Dir['db/migrate/*.rb']
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.required_ruby_version = ">= 3.2.0"

  spec.add_dependency "sidekiq", ">= 8", "< 9"
  spec.add_dependency "colorize"
  spec.add_dependency "activerecord", ">= 6.0"
  spec.add_dependency "activesupport", ">= 6.0"

  spec.add_development_dependency "bundler", "~> 2.1"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "sqlite3", ">= 2.1"
end

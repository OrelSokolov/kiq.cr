# frozen_string_literal: true

namespace :sidekiq_dags do
  desc "Copy migrations to Rails app"
  task :install do
    source = File.expand_path("../../db/migrate", __FILE__)
    destination = Rails.root.join("db/migrate")
    
    Dir.glob("#{source}/*.rb").each do |file|
      filename = File.basename(file)
      dest_file = destination.join(filename)
      
      if File.exist?(dest_file)
        puts "Migration #{filename} already exists, skipping..."
      else
        FileUtils.cp(file, dest_file)
        puts "Copied migration #{filename}"
      end
    end
  end
end


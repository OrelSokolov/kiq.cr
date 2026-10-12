# Kiq::Middleware — Rack-style client/server middleware chains, 1:1 with
# Sidekiq::Middleware::Chain (add/prepend/remove/exists?/clear + invoke).
#
# Deviation: the chain stores middleware *instances* (instantiated once
# per chain), sidekiq instantiates per job. Sidekiq job instances are
# per-execution too; Crystal jobs are instantiated per execution — the
# chain reuse is safe because Crystal middleware is expected to be
# stateless across invocations.

module Kiq
  module Middleware
    # Client middleware: runs at push time. Must return the (possibly
    # modified) msg, or nil to abort the push. `yield` runs the rest of
    # the chain.
    module ClientMiddleware
      abstract def call(job_class : String, msg : Msg, queue : String, & : -> Msg?) : Msg?
    end

    # Server middleware: runs around job execution. The job instance is
    # typed as Kiq::AnyJob — the union of all registered job classes,
    # generated from the compile-time registry (Crystal 1.x has no
    # `Module+` restriction in def params).
    module ServerMiddleware
      abstract def call(job : Kiq::AnyJob, msg : Msg, queue : String, & : -> Nil) : Nil
    end

    class Chain
      private alias ClientRunner = Proc(String, Msg, String, Proc(Msg?), Msg?)
      private alias ServerRunner = Proc(Kiq::AnyJob, Msg, String, Proc(Nil), Nil)

      @client_entries = [] of {String, ClientMiddleware}
      @server_entries = [] of {String, ServerMiddleware}

      def add(middleware) : Chain
        case middleware
        when ClientMiddleware
          @client_entries << {label(middleware), middleware}
        when ServerMiddleware
          @server_entries << {label(middleware), middleware}
        else
          raise ArgumentError.new("#{middleware.class} includes neither Kiq::Middleware::ClientMiddleware nor ServerMiddleware")
        end
        self
      end

      def prepend(middleware) : Chain
        case middleware
        when ClientMiddleware
          @client_entries.unshift({label(middleware), middleware})
        when ServerMiddleware
          @server_entries.unshift({label(middleware), middleware})
        end
        self
      end

      def remove(middleware_class : Class) : Chain
        name = middleware_class.name.not_nil!
        @client_entries.reject! { |label, _| label == name }
        @server_entries.reject! { |label, _| label == name }
        self
      end

      def exists?(middleware_class : Class) : Bool
        name = middleware_class.name.not_nil!
        (@client_entries + @server_entries).any? { |label, _| label == name }
      end

      def clear : Chain
        @client_entries.clear
        @server_entries.clear
        self
      end

      def invoke_client(job_class : String, msg : Msg, queue : String, &last : -> Msg?) : Msg?
        inner = ->(m : Msg) { last.call }
        @client_entries.reverse_each do |_, mw|
          prev = inner
          runner = mw
          inner = ->(m : Msg) { runner.call(job_class, m, queue) { prev.call(m) } }
        end
        inner.call(msg)
      end

      def invoke_server(job : Kiq::AnyJob, msg : Msg, queue : String, &last : -> Nil) : Nil
        inner = ->(m : Msg) { last.call }
        @server_entries.reverse_each do |_, mw|
          prev = inner
          runner = mw
          inner = ->(m : Msg) { runner.call(job, m, queue) { prev.call(m) } }
        end
        inner.call(msg)
      end

      private def label(middleware) : String
        middleware.class.name.to_s
      end
    end
  end
end

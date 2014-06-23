require "em/warden/client"

module Dea
  class Container
    class ConnectionError < StandardError; end

    attr_reader :handle, :socket_path

    def initialize(handle, socket_path)
      @handle = handle
      @socket_path = socket_path
      @warden_connections = {}
    end

    def info
      `matrix_query #{@handle}`
    end

    private

    def client
      @client ||=
        EventMachine::Warden::FiberAwareClient.new(@socket_path).tap(&:connect)
    end
  end
end

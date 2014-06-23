# coding: UTF-8

require "em/warden/client/connection"
require "steno"
require "steno/core_ext"
require "dea/promise"
require "vmstat"
require "fileutils"

module Dea
  class Task
    class BaseError < StandardError; end
    class WardenError < BaseError; end
    class NotImplemented < StandardError; end

    attr_reader :config
    attr_reader :logger

    def initialize(config, custom_logger=nil)
      @config = config
      @logger = custom_logger || self.class.logger.tag({})
    end

    MATRIX_BASE = "/home/matrix/containers"
    MATRIX_JAIL = "matrix_jail"    

    def container_path(id, dst)
      MATRIX_BASE + "/" + id + "/" + dst
    end

    def promise_copy_in(id, src, dst, privilege=false)
      return false unless (id && src && dst)
      p "src #{src} dst #{container_path(id, dst)}"
      FileUtils.cp src , container_path(id, dst)
      FileUtils.chown_R('work', 'work', container_path(id, dst)) unless privilege
      return true
    end
 
    def promise_copy_out(id, src, dst)
      return false unless (id && src && dst)
      FileUtils.cp container_path(id, src), dst
      return true
    end
  
    def promise_matrix_run(id, script)
      return false unless id
      `#{MATRIX_JAIL} #{id}  "#{script}"`
    end

    def container_handle
      @attributes["matrix_container"]
    end

    def promise_container_info
      Promise.new do |p|
        #use matrix_query
      end
    end

    def promise_destroy
      Promise.new do |p|
        attributes.delete("matrix_container")

        p.deliver
      end
    end

    # Resolve a promise making sure that only one runs at a time.
    def resolve(p, name)
      if @busy
        logger.warn("Ignored: #{name}")
        return
      else
        @busy = true

        Promise.resolve(p) do |error, result|
          begin
            took = "took %.3f" % p.elapsed_time

            if error
              logger.warn("Failed: #{name} (#{took})")
              logger.log_exception(error)
            else
              logger.info("Delivered: #{name} (#{took})")
            end

            yield(error, result)
          ensure
            @busy = false
          end
        end
      end
    end

  end
end

# coding: UTF-8
require "membrane"
require "steno"
require "steno/core_ext"
require "vcap/common"
require "yaml"

require "dea/container"
require "dea/env"
require "dea/health_check/port_open"
require "dea/health_check/state_file_ready"
require "dea/promise"
require "dea/stat_collector"
require "dea/task"
require "dea/utils/event_emitter"

module Dea
  class Instance < Task
    include EventEmitter

    STAT_COLLECTION_INTERVAL_SECS = 10
    DEFAULT_APPWORKSPACE_USER = "default"
    DEFAULT_WORKUSER_LENGTH = 30
    DEFAULT_WORKUSER_PASSWORD = 'default'

    BIND_MOUNT_MODE_MAP = {
      "ro" =>  ::Warden::Protocol::CreateRequest::BindMount::Mode::RO,
      "rw" =>  ::Warden::Protocol::CreateRequest::BindMount::Mode::RW,
    }

    class State
      BORN     = "BORN"
      STARTING = "STARTING"
      RUNNING  = "RUNNING"
      STOPPING = "STOPPING"
      STOPPED  = "STOPPED"
      CRASHED  = "CRASHED"
      DELETED  = "DELETED"
      RESUMING = "RESUMING"

      def self.from_external(state)
        case state.upcase
        when "BORN"
          BORN
        when "STARTING"
          STARTING
        when "RUNNING"
          RUNNING
        when "STOPPING"
          STOPPING
        when "STOPPED"
          STOPPED
        when "CRASHED"
          CRASHED
        when "DELETED"
          DELETED
        when "RESUMING"
          RESUMING
        else
          raise "Unknown state: #{state}"
        end
      end

      def self.to_external(state)
        case state
        when Dea::Instance::State::BORN
          "BORN"
        when Dea::Instance::State::STARTING
          "STARTING"
        when Dea::Instance::State::RUNNING
          "RUNNING"
        when Dea::Instance::State::STOPPING
          "STOPPING"
        when Dea::Instance::State::STOPPED
          "STOPPED"
        when Dea::Instance::State::CRASHED
          "CRASHED"
        when Dea::Instance::State::DELETED
          "DELETED"
        when Dea::Instance::State::RESUMING
          "RESUMING"
        else
          raise "Unknown state: #{state}"
        end
      end
    end

    class Transition < Struct.new(:from, :to)
      def initialize(*args)
        super(*args.map(&:to_s).map(&:downcase))
      end
    end

    class TransitionError < BaseError
      attr_reader :from
      attr_reader :to

      def initialize(from, to = nil)
        @from = from
        @to = to
      end

      def message
        parts = []
        parts << "Cannot transition from %s" % [from.inspect]

        if to
          parts << "to %s" % [to.inspect]
        end

        parts.join(" ")
      end
    end

    def self.translate_attributes(attributes)
      attributes = attributes.dup

      attributes["instance_index"]      ||= attributes.delete("index")

      attributes["application_id"]      ||= attributes.delete("droplet").to_s
      attributes["tags"]                ||= attributes.delete("tags") { |_| {} }
      attributes["application_version"] ||= attributes.delete("version")
      attributes["application_name"]    ||= attributes.delete("name")
      attributes["application_uris"]    ||= attributes.delete("uris")
      attributes["application_prod"]    ||= attributes.delete("prod")

      attributes["droplet_sha1"]        ||= attributes.delete("sha1")
      attributes["droplet_uri"]         ||= attributes.delete("executableUri")
      attributes["infohash"]         ||= attributes.delete("infohash")
      attributes["application_space"]         ||= attributes["tags"]["space_name"]
      attributes["application_org"]         ||= attributes["tags"]["org_name"]
      attributes["application_name_without_version"]         ||= attributes["application_name"].split("_")[0]
      attributes["use_p2p"]         ||= attributes.delete("use_p2p")

      # Translate environment to dictionary (it is passed as Array with VAR=VAL)
      env = attributes.delete("env") || []
      attributes["environment"] ||= Hash[env.map do |e|
        pair = e.split("=", 2)
        pair[0] = pair[0].to_s
        pair[1] = pair[1].to_s
        pair
      end]

      attributes
    end

    def self.limits_schema
      Membrane::SchemaParser.parse do
        {
          "mem"  => Fixnum,
          "disk" => Fixnum,
          "fds"  => Fixnum,
        }
      end
    end

    def self.service_schema
      Membrane::SchemaParser.parse do
        {
          "name"        => String,
          "label"        => String,
          "credentials" => any,

          # Deprecated fields
          optional("plan")        => String,
          optional("vendor")      => String,
          optional("version")     => String,
          optional("type")        => String,
          optional("tags")        => [String],
          optional("plan_option") => enum(String, nil),
        }
      end
    end

    def self.schema
      limits_schema = self.limits_schema
      service_schema = self.service_schema

      Membrane::SchemaParser.parse do
        {
          # Static attributes (coming from cloud controller):
          "cc_partition"        => String,

          "instance_id"         => String,
          "instance_index"      => Integer,

          "application_id"      => String,
          "application_version" => String,
          "application_name"    => String,
          "application_uris"    => [String],
          "application_prod"    => bool,

          "droplet_sha1"        => enum(nil, String),
          "droplet_uri"         => enum(nil, String),
          "infohash"         => enum(nil, String),
          "use_p2p"         => bool,
          "application_space"    => String,
          "application_org"    => String,
          "application_name_without_version"    => String,

          optional("tags")                 => dict(String, any),
          optional("runtime_name")         => String,
          optional("runtime_info")         => dict(String, any),
          optional("framework_name")       => String,

          # TODO: use proper schema
          "limits"              => limits_schema,
          "environment"         => dict(String, String),
          "services"            => [service_schema],
          optional("flapping")  => bool,

          optional("debug")     => enum(nil, String),
          optional("console")   => enum(nil, bool),
          optional("instance_meta") => hash,
          # private_instance_id is internal id that represents the instance,
          # which is generated by DEA itself. Currently, we broadcast it to
          # all routers. Routers use that as sticky session of the instance.
          "private_instance_id" => String,
        }
      end
    end

    # Define an accessor for every attribute with a schema
    self.schema.schemas.each do |key, _|
      define_method(key) do
        attributes[key]
      end
    end

    # Accessors for different types of host/container ports
    [nil, "debug", "console"].each do |type|
      ["host", "container"].each do |side|
        key = ["instance", type, side, "port"].compact.join("_")
        define_method(key) do
          attributes[key]
        end
      end
    end

    def self.define_state_methods(state)
      state_predicate = "#{state.to_s.downcase}?"
      define_method(state_predicate) do
        self.state == state
      end

      state_time = "state_#{state.to_s.downcase}_timestamp"
      define_method(state_time) do
        attributes[state_time]
      end
    end

    # Define predicate methods for querying state
    State.constants.each do |state|
      define_state_methods(State.const_get(state))
    end

    attr_reader :bootstrap
    attr_reader :attributes
    attr_accessor :exit_status
    attr_accessor :exit_description

    def initialize(bootstrap, attributes, app_user = nil)
      super(bootstrap.config)
      @bootstrap = bootstrap

      @attributes = attributes.dup
      @attributes["application_uris"] ||= []

      # Generate unique ID
      @attributes["instance_id"] ||= VCAP.secure_uuid

      # Contatenate 2 UUIDs to genreate a 32 chars long private_instance_id
      @attributes["private_instance_id"] ||= VCAP.secure_uuid + VCAP.secure_uuid

      self.state = State::BORN

      # Assume non-production app when not specified
      @attributes["application_prod"] ||= false

      @app_user = app_user

      @exit_status           = -1
      @exit_description      = ""
    end

    def setup
      setup_stat_collector
      #setup_link
      setup_crash_handler
    end

    # TODO: Fill in once start is hooked up
    def flapping?
      false
    end

    def app_workspace_user
      @app_user ? @app_user : DEFAULT_APPWORKSPACE_USER
    end

    def memory_limit_in_bytes
      limits["mem"].to_i * 1024 * 1024
    end

    def disk_limit_in_bytes
      limits["disk"].to_i * 1024 * 1024
    end

    def file_descriptor_limit
      limits["fds"].to_i
    end

    def production_app?
      attributes["application_prod"]
    end

    def instance_path_available?
      state == State::RUNNING || state == State::CRASHED
    end

    def consuming_memory?
      case state
      when State::BORN, State::STARTING, State::RUNNING, State::STOPPING
        true
      else
        false
      end
    end

    def consuming_disk?
      case state
      when State::BORN, State::STARTING, State::RUNNING, State::STOPPING,
           State::CRASHED
        true
      else
        false
      end
    end

    def instance_path
      attributes["instance_path"] ||=
        begin
          if !instance_path_available? || attributes["warden_container_path"].nil?
            raise "Instance path unavailable"
          end

          File.expand_path(container_relative_path(attributes["warden_container_path"]))
        end
    end

    def validate
      self.class.schema.validate(@attributes)
    end

    def state
      attributes["state"]
    end

    def state=(state)
      transition = Transition.new(attributes["state"], state)

      attributes["state"] = state
      attributes["state_timestamp"] = Time.now.to_f

      state_time = "state_#{state.to_s.downcase}_timestamp"
      attributes[state_time] = Time.now.to_f

      emit(transition)
    end

    def state_timestamp
      attributes["state_timestamp"]
    end

    def droplet
      bootstrap.droplet_registry[droplet_sha1]
    end

    def application_uris=(uris)
      attributes["application_uris"] = uris
      nil
    end

    def to_s
      "Instance(id=%s, idx=%s, app_id=%s)" % [instance_id.slice(0, 4),
                                             instance_index, application_id]
    end

    def promise_state(from, to = nil)
      Promise.new do |p|
        if !Array(from).include?(state)
          p.fail(TransitionError.new(state, to))
        else
          if to
            self.state = to
          end

          p.deliver
        end
      end
    end

    def promise_droplet_download
      Promise.new do |p|
        droplet.download(droplet_uri) do |error|
          if error
            p.fail(error)
          else
            p.deliver
          end
        end
      end
    end

    def promise_extract_droplet
      Promise.new do |p|
        script = []
        script << "cd /home/work"
        script << "tar zxf #{droplet.droplet_basename}"
        script << "rsync -auP app/* /home/work"
        #script << "find . -type f -maxdepth 1 | xargs chmod og-x"
        script = script.join(' && ')
        p "script #{script}"
        promise_matrix_run(attributes['matrix_container'], script)
        p.deliver
      end
    end

    def metadata(opts={})
      {
        "app_uri" => opts[:uris],
        "app_id" => attributes['application_db_id'].to_s,
        "app_name" => attributes['application_name'],
        "instance_ip" => bootstrap.local_ip,
        "instance_id" => attributes['instance_id'],
        "instance_index" => attributes['instance_index'].to_s,
        "instance_meta"  => attributes['instance_meta'],
        "instance_tags"  => attributes['tags']
      }
    end

    def parse_droplet_metadata()
      begin
        @attributes['instance_meta'] = promise_read_instance_manifest(attributes['matrix_container']).resolve || {}
        if ( config['enable_sshd'] == true )
          @attributes['instance_meta']['raw_ports'] = {} if !@attributes['instance_meta']['raw_ports']
          log(:warn, "ignore user defined sshd port") if @attributes['instance_meta']['raw_ports']['sshd']
	  @attributes['instance_meta']['raw_ports']['sshd']={'port' => 22, 'http' => false, 'bns' => true} 
        end
      rescue => e
        log(:warn, "parse droplet metadata failed with exception #{e}")
        @attributes['instance_meta'] = {}
      end
    end

    def promise_start
      Promise.new do |p|
        p "begin start"
        p attributes['instance_meta']
        attributes['instance_meta']['prod_ports'].each_pair do |key, value|
          env_key = attributes['tags']['bns_node'] + '_' + key
          env_val = value['port']
          #script << "export %s=%s" % [env_key, env_val]
        end        

        script = []
        script << "mkdir -p /home/work/jpaas_run/logs"
        script << "cd /home/work"
        startup = "./startup "
        
        if self.instance_container_port
          startup += " -p %d" % self.instance_container_port
        end
        startup += "&"
        script << startup
        script = script.join(" && ")
        p "script #{script}"
        response = promise_matrix_run(attributes['matrix_container'], script)

        p.deliver
      end
    end

    def start_with_matrix(&callback) 
      p = Promise.new do |p|
         attributes['matrix_container'] = attributes['instance_index'].to_s + '.' + attributes['tags']['bns_node']
         promise_droplet.resolve
         parse_droplet_metadata
         msg = matrix_create_container_msg(attributes)
         apply_container(msg) do |resp|
           resp = resp.data
           return if resp['err']
           setup_network(resp['ports'])
           start(&callback)
         end
         p.deliver
      end
      resolve(p, "start instance with matrix") do |error, _|
        
      end       
    end

    def setup_network(ports)
      return unless ports.size > 0
      raw_ports = attributes['instance_meta']['raw_ports']
      if raw_ports
        prod_ports = {}
        attributes['instance_meta']['prod_ports'] = {}
        raw_ports.each_pair do |name, info|
          port = ports[name]
          prod_ports[name] = {
            'host_port'=> port,
            'container_port' => port,
            'port_info' => info
          }
          if "true" == info['http'].to_s
            attributes["instance_host_port"] = port
            attributes["instance_container_port"] = port
          end
        end
        attributes['instance_meta']['prod_ports'] = prod_ports
      end
      p attributes['instance_meta']['prod_ports']
    end

    def start(&callback)
      p = Promise.new do
        log(:info, "droplet.starting")
        promise_state(State::BORN, State::STARTING).resolve

        promise_cpin_droplet.resolve

        [
          promise_extract_droplet,
          promise_start
        ].each(&:resolve)

        on(Transition.new(:starting, :crashed)) do
          cancel_health_check
        end

        # Fire off link so that the health check can be cancelled when the
        # instance crashes before the health check completes.
        #link

        if promise_health_check.resolve
          promise_state(State::STARTING, State::RUNNING).resolve
          log(:info, "droplet.healthy")
        else
          log(:warn, "droplet.unhealthy")
          p.fail("App instance failed health check")
        end

        p.deliver
      end

      resolve(p, "start instance") do |error, _|
        if error
          # An error occured while starting, mark as crashed
          self.exit_description = error.message
          self.state = State::CRASHED
        end

        callback.call(error) unless callback.nil?
      end
    end

    def matrix_create_container_msg(msg)
        {
          "service_name"   => msg['tags']['bns_node'],
          "offset"         => msg['instance_index'],
          "ip"             => bootstrap.local_ip,
          "payload"        => {
            "packageSource" => "",
            "packageVersion" => "",
            "packageType" => "EMPTY",
            "deployDir" => "/home/work",
            "process" => {
               "main" => "java"
             },
            "tag" => {
               "tag1" => "value1"
            },
            "deployTimeoutSec" => 300,
            "healthCheckTimeoutSec" => 30,
            "enableHealthCheck" => false,
            "resourceRequirement" => {
               "cpu" => {
                  "normalizedCore" => {
                     "quota" => 1,
                     "limit" => 2
                   }
               },
               "memory" => {
                  "sizeMB" => {
                     "quota" => msg['limits']['mem'],
                     "limit" => msg['limits']['mem']
                   }
               },
               "network" => {
                  "inBandwidthMBPS" => {
                     "quota" => 10,
                     "limit" => 10
                   },
                   "outBandwidthMBPS" => {
                      "quota" => 1,
                      "limit" => 1
                   }
                },
               "port" => {
                  "staticPort" => get_static_ports,
                  "dynamicPortName" => get_dynamic_ports
               },
               "process" => {
                  "thread" => {
                     "quota" => 1000,
                     "limit" => 1000
                   }
               },
               "workspace" => {
                  "sizeMB" => {
                     "quota" => msg['limits']['disk'],
                     "limit" => msg['limits']['disk']
                  },
                  "inode" => {
                      "quota" => 10000,
                      "limit" => 10000
                   },
                  "type" => "home",
                  "bindPoint" => "data2",
                  "exclusive" => false,
                  "useSoftLinkDir" => false
               },
               "requiredDisk" => [],
               "optionalDisk" => []
            },
            "baseEnv" => "centos6u3"
          }
        }
    end

    def get_static_ports
       static_ports = {}
       attributes['instance_meta']['raw_ports'].each_pair do |name, info|
         static_ports[name] = info['port']
       end
       #static_ports
       {}
    end

    def get_dynamic_ports
       dynamic_ports = []
       attributes['instance_meta']['raw_ports'].each_pair do |name, info|
         dynamic_ports << name
       end
       dynamic_ports
    end

    def apply_container(msg, &blk)
        #matrix_msg = matrix_create_container_msg(msg)
        sid = bootstrap.nats.request("matrix.container.create", msg) do |resp, error|
          blk.call(resp) unless error != nil
        end
        #bootstrap.nats.timeout(sid, 3) do 
        #  log(:warn, "failed create container")
        #end
    end


    def promise_cpin_droplet
      Promise.new do |p|
          p "when cpy in #{droplet.droplet_path} to /home/work"
          start = Time.now
          promise_copy_in(attributes["matrix_container"],
                          droplet.droplet_path,
                          "/home/work")
          log(:info, "droplet.download.finished", :took => Time.now - start)
        p.deliver
      end
    end

    def promise_droplet
      Promise.new do |p|
        if !droplet.droplet_exist?
          log(:info, "droplet.download.starting")
          start = Time.now
          promise_droplet_download.resolve
          FileUtils.makedirs(droplet.droplet_unzip_dirname)
          p "tar zxf #{droplet.droplet_path} -C #{droplet.droplet_unzip_dirname}"
          `tar zxf #{droplet.droplet_path} -C #{droplet.droplet_unzip_dirname}`
          log(:info, "droplet.download.finished", :took => Time.now - start)
        else
          log(:info, "droplet.download.skipped")
        end
        p.deliver
      end
    end

    def stop(&callback)
      p = Promise.new do
        log(:info, "droplet.stopping")

        promise_state(State::RUNNING, State::STOPPING).resolve

        promise_stop.resolve

        promise_state(State::STOPPING, State::STOPPED).resolve

        p.deliver
      end

      resolve(p, "stop instance") do |error, _|
        callback.call(error) unless callback.nil?
      end
    end

    def setup_crash_handler
      # Resuming to crashed state
      on(Transition.new(:resuming, :crashed)) do
        crash_handler
      end

      # On crash
      on(Transition.new(:starting, :crashed)) do
        crash_handler
      end

      # On crash
      on(Transition.new(:running, :crashed)) do
        crash_handler
      end
    end

    def promise_crash_handler
      Promise.new do |p|
        if attributes["matrix_container"]
          promise_destroy.resolve
        end

        p.deliver
      end
    end

    def crash_handler(&callback)
      Promise.resolve(promise_crash_handler) do |error, _|
        if error
          log(
            :warn, "droplet.crash-handler.error",
            :error => error, :backtrace => error.backtrace)
        end

        callback.call(error) unless callback.nil?
      end
    end

    def setup_stat_collector
      on(Transition.new(:resuming, :running)) do
        log(:warn, "begin to start stat collector from :resuming, :running")
        stat_collector.start
      end

      on(Transition.new(:starting, :running)) do
        log(:warn, "begin to start stat collector")
        stat_collector.start
      end

      on(Transition.new(:running, :stopping)) do
        stat_collector.stop
      end

      on(Transition.new(:running, :crashed)) do
        stat_collector.stop
      end
    end

    def promise_read_instance_manifest(container_path)
      Promise.new do |p|
        p 'read instance manifest'
        p container_path
        if container_path.nil?
          p.deliver({})
          next
        end
        manifest_path = metafile_relative_path(container_path)
        p "manifest_path #{manifest_path}"
        if !File.exist?(manifest_path)
          p.deliver({})
        else
          manifest = YAML.load_file(manifest_path)
          p.deliver(manifest)
        end
      end
    end

    def promise_port_open(port, timeout)
      Promise.new do |p|
        host = bootstrap.local_ip

        log(:debug, "droplet.healthcheck.port", :host => host, :port => port)

        @health_check = Dea::HealthCheck::PortOpen.new(host, port) do |hc|
          hc.callback { p.deliver(true) }

          hc.errback  { p.deliver(false) }

          if attributes["debug"] != "suspend"
            hc.timeout(timeout)
          end
        end
      end
    end

    def promise_state_file_ready(path, timeout)
      Promise.new do |p|
        log(:debug, "droplet.healthcheck.file", :path => path)

        @health_check = Dea::HealthCheck::StateFileReady.new(path) do |hc|
          hc.callback { p.deliver(true) }

          hc.errback { p.deliver(false) }

          if attributes["debug"] != "suspend"
            hc.timeout(timeout)
          end
        end
      end
    end

    def cancel_health_check
      if @health_check
        @health_check.fail
        @health_check = nil
      end
    end

    def promise_health_check
      Promise.new do |p|
        manifest = promise_read_instance_manifest(attributes['matrix_container']).resolve
        p manifest
        if manifest && manifest["start_timeout"]
           start_timeout = manifest["start_timeout"]
        else
          start_timeout = 300
        end
        if manifest && manifest["state_file"]
          p "check stat file"
          manifest_path = container_relative_path(attributes['matrix_container'], "/home/work/",manifest["state_file"])
          p.deliver(promise_state_file_ready(manifest_path, start_timeout).resolve)
        elsif !application_uris.empty?
          attributes["instance_host_port"] = attributes['instance_meta']['raw_ports'].values_at(1)['port']
          p.deliver(promise_port_open(attributes["instance_host_port"], start_timeout).resolve)
        else
          p.deliver(true)
        end
      end
    end

    def used_memory_in_bytes
      stat_collector.used_memory_in_bytes
    end

    def used_disk_in_bytes
      stat_collector.used_disk_in_bytes
    end

    def computed_pcpu
      stat_collector.computed_pcpu
    end

    def container
      @container ||= Dea::Container.new(@attributes["warden_handle"], config["warden_socket"])
    end

    def stat_collector
      @stat_collector ||= StatCollector.new(container)
    end

    private

    def determine_exit_description(link_response)
      info = link_response.info
      return "cannot be determined" unless info

      if info.events && info.events.include?("oom")
        return "out of memory"
      end

      "app instance exited"
    end

    def container_relative_path(handle, *parts)
      File.join("/home/matrix/containers", handle, *parts)
    end

    def metafile_relative_path(container_path)
      p "unzip #{droplet.droplet_unzip_dirname}"
      File.join(droplet.droplet_unzip_dirname, 'droplet.yaml')
    end

    def logger
      tags = {
        "instance_id"         => instance_id,
        "instance_index"      => instance_index,
        "application_id"      => application_id,
        "application_version" => application_version,
        "application_name"    => application_name,
      }

      @logger ||= self.class.logger.tag(tags)
    end

    def log(level, message, data = {})
      logger.send(level, message, base_log_data.merge(data))
    end

    def base_log_data
      { :attributes => @attributes }
    end
  end
end

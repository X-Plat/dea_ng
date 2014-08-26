# coding: UTF-8

require "digest/sha1"
require "em-http"
require "fileutils"
require "steno"
require "steno/core_ext"
require "tempfile"
require "dea/utils/download"

module Dea
  class Droplet
    attr_reader :base_dir
    attr_reader :sha1
    attr_reader :app_name
    attr_reader :app_space
    attr_reader :app_org
    attr_reader :app_workuser
    attr_reader :app_workdir

    def initialize(base_dir, sha1, app_workuser, app_workdir)
      @base_dir = base_dir
      @sha1 = sha1
      @app_workuser = app_workuser
      @app_workdir = app_workdir

      # Make sure the directory exists
      FileUtils.mkdir_p(droplet_dirname)
    end

    def droplet_dirname
      File.expand_path(File.join(base_dir, sha1))
    end

    def droplet_basename
      "droplet.tgz"
    end

    def droplet_dirname_in_container
      "/home/#{app_workuser}/#{app_workdir}/droplets"
    end

    def droplet_path_in_container
      File.join(droplet_dirname_in_container, droplet_basename)
    end

    def droplet_path
      File.join(droplet_dirname, droplet_basename)
    end

    def droplet_exist?
      File.exist?(droplet_path)
    end

    def download(uri, &blk)
      @download_waiting ||= []
      @download_waiting << blk

      logger.debug "Waiting for download to complete"

      if @download_waiting.size == 1
        # Fire off request when this is the first call to #download
        Download.new(uri, droplet_dirname, sha1).download! do |err, path|
          if !err
            File.rename(path, droplet_path)
            File.chmod(0755, droplet_path)

            logger.debug "Moved droplet to #{droplet_path}"
          end

          while blk = @download_waiting.shift
            blk.call(err)
          end
        end
      end
    end
    def unzip_droplet_dir
        File.join(base_dir,"../unzip_droplet")
    end

    def seed_file(infohash)
        File.join(base_dir,"../#{infohash}.torrent")
    end

    def get_os
      logger.debug "Start to get os type from Release directly"
      rel_info = `tar zxfO #{droplet_path} ./app/Release`
      logger.info("Release #{rel_info}")
      if $?.success?
        os_type = YAML.load(rel_info)["os"]
        return os_type if os_type
      end
      return nil
    end
 
    def get_os_p2p(sub_dir)
      logger.debug "Start to get os type from Release directly at p2p mod"
      rel_info = `cat #{unzip_droplet_dir}/#{sub_dir}/app/Release`
      if $?.success?
        os_type = YAML.load(rel_info)["os"]
        return os_type if os_type
      end
      return nil
    end

    def download_unzip_droplet(infohash,&blk)
      @download_waiting ||= []
      @download_waiting << blk

      logger.debug "Waiting for download to complete by gko3"

      if @download_waiting.size == 1
        begin
            # Fire off request when this is the first call to #download
            unzip_droplet_dir=File.join(base_dir,"../unzip_droplet")
            FileUtils.mkdir_p(unzip_droplet_dir) unless File.exists?(unzip_droplet_dir)
            system("gko3 sdown -i #{infohash} -p #{unzip_droplet_dir} -d 20 -u 20 --seedtime 5 --hang-timeout 10 --save-torrent #{seed_file(infohash)}")
            if $?.success?
                err=nil
                logger.debug "Download unzip droplet to #{unzip_droplet_dir}"
                #delete extra files if necessary
                system("gko3 rmfiles -p #{unzip_droplet_dir} -r #{seed_file(infohash)} --not-in")
                if $?.success?
                    err=nil
                    logger.debug "delete extra files ok"
                else
                    err="Failed to delete extra files"
                end
            else
                err="Failed to download unzip droplet:gko3 sdown -i #{infohash} -p #{unzip_droplet_dir}"
            end
            while blk = @download_waiting.shift
                blk.call(err)
            end
        ensure
            system("rm -f #{seed_file(infohash)}")
        end
      end
    end

    def local_copy(source, &blk)
      logger.debug "Copying local droplet to droplet registry"
      begin
        FileUtils.cp(source, droplet_path)
        blk.call
      rescue => e
        blk.call(e)
      end
    end

    def destroy(&callback)
      dir_to_remove = droplet_dirname + ".deleted." + Time.now.to_i.to_s

      # Rename first to both prevent a new instance from referencing a file
      # that is about to be deleted and to avoid doing a potentially expensive
      # operation on the reactor thread.
      logger.debug("Renaming #{droplet_dirname} to #{dir_to_remove}")
      File.rename(droplet_dirname, dir_to_remove)

      operation = lambda do
        logger.debug("Removing #{dir_to_remove}")

        begin
          FileUtils.rm_rf(dir_to_remove)
        rescue => e
          logger.log_exception(e)
        end
      end

      EM.defer(operation, callback)
    end

    private

    def logger
      @logger ||= self.class.logger.tag(:droplet_sha1 => sha1)
    end
  end
end

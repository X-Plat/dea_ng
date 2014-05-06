module Dea
  class StartupScriptGenerator
   
    def self.strip_heredoc(string)
      indent = string.scan(/^[ \t]*(?=\S)/).min.try(:size) || 0
      string.gsub(/^[ \t]{#{indent}}/, '')
    end

    EXPORT_BUILDPACK_ENV_VARIABLES_SCRIPT = strip_heredoc(<<-BASH).freeze
      unset GEM_PATH
      if [ -d .node/.profile.d ]; then
        for i in .node/.profile.d/*.sh; do
          if [ -r $i ]; then
            . $i
          fi
        done
        unset i
      fi
    BASH

    START_SCRIPT = strip_heredoc(<<-BASH).freeze
      DROPLET_BASE_DIR=$PWD
      (%s) > >(tee $DROPLET_BASE_DIR/.node/logs/stdout.log) 2> >(tee $DROPLET_BASE_DIR/.node/logs/stderr.log >&2) &
      STARTED=$!
      echo "$STARTED" >> $DROPLET_BASE_DIR/.node/run.pid

      wait $STARTED
    BASH

    def initialize(start_command, user_envs, system_envs, app_workuser, app_workdir)
      @start_command = start_command
      @user_envs = user_envs
      @system_envs = system_envs
      @app_workuser = app_workuser
      @app_workdir = app_workdir
    end

    def generate
      script = []
      script << "umask 077"
      script << @system_envs
      script << EXPORT_BUILDPACK_ENV_VARIABLES_SCRIPT
      script << @user_envs
      script << "env > .node/logs/env.log"
      script << START_SCRIPT % @start_command
      script.join("\n")
      p script.join("\n")
      return script.join("\n")
    end
  end
end

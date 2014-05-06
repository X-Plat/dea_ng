module Dea
  class StartupScriptGenerator
   
    def self.strip_heredoc(string)
      indent = string.scan(/^[ \t]*(?=\S)/).min.try(:size) || 0
      string.gsub(/^[ \t]{#{indent}}/, '')
    end

    EXPORT_BUILDPACK_ENV_VARIABLES_SCRIPT = strip_heredoc(<<-BASH).freeze
      unset GEM_PATH
      WORK_DIR=%s/app
      if [ -d ${WORK_DIR}/.profile.d ]; then
        for i in ${WORK_DIR}/.profile.d/*.sh; do
          if [ -r $i ]; then
            . $i
          fi
        done
        unset i
      fi
    BASH

    START_SCRIPT = strip_heredoc(<<-BASH).freeze
      DROPLET_BASE_DIR=$PWD
      DROPLET_RUN_DIR=${DROPLET_BASE_DIR}/%s
      mkdir -p ${DROPLET_RUN_DIR}/{logs,status}
      (%s) > >(tee ${DROPLET_RUN_DIR}/logs/stdout.log) 2> >(tee ${DROPLET_RUN_DIR}/logs/stderr.log >&2) &
      STARTED=$!
      echo "$STARTED" >> ${DROPLET_RUN_DIR}/status/run.pid

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
      script << EXPORT_BUILDPACK_ENV_VARIABLES_SCRIPT % @app_workdir
      script << @user_envs
      script << "env > #{@app_workdir}/logs/env.log"
      script << START_SCRIPT % [ @app_workdir, @start_command ]
      script.join("\n")
      p script.join("\n")
      return script.join("\n")
    end
  end
end

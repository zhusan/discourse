# examples: https://github.com/capistrano/sshkit/blob/master/EXAMPLES.md  
# sshkit
set :application, 'discourse'

set :scm, :git
set :repo_url, 'git@github.com:zhusan/discourse.git'
set :deploy_via, :remote_cache
set :copy_exclude, %w{.git}     # cache repo in shared/cached-copy

set :keep_releases, 5
# set :bundle_cmd, 'source $HOME/.bash_profile && bundle'
set :bundle_bins, fetch(:bundle_bins, []).push(:unicorn)
set :bundle_jobs, 4 #This is only available for bundler 1.4+

set :linked_files, %w{config/database.yml config/discourse_defaults.conf config/discourse.pill config/secrets.yml}
set :linked_dirs, %w{bin log tmp/pids tmp/cache tmp/sockets vendor/bundle public/uploads}

%w[redis nginx].each do |service|
  namespace service do
    %w[start stop restart status].each do |command|
      desc "#{command} #{service}"
      task command do
        on roles(:app) do
          sudo "service #{service} #{command}", raise_on_non_zero_exit: false
        end
      end
    end
  end
end

namespace :db do
  %w[create migrate reset rollback seed setup drop version].each do |command|
    desc "rake db:#{command}"
    task command do
      on roles(:app) do
        with rails_env: fetch(:rails_env) do
          within "#{deploy_to}/current" do
            execute :rake, "db:#{command}"
          end
        end
      end
    end
  end

  desc "show the content of database config"
  task :info do
    on roles(:app) do
      execute "cd #{deploy_to}/current/ && cat config/database.yml"
    end
  end
end

namespace :assets do
  %w[clobber environment precompile].each do |command|
    desc "rake assets:#{command}"
    task command do
      on roles(:app) do
        with rails_env: fetch(:rails_env) do
          within "#{deploy_to}/current" do
            as :discourse do
              execute :rake, "assets:#{command}"
            end
          end
        end
      end
    end
  end
end

namespace :unicorn do

  desc "start unicorn"
  task :start do
    on roles(:app) do
      with rails_env: fetch(:rails_env) do
        within "#{deploy_to}/current" do
          if test("[ -f #{deploy_to}/current/tmp/pids/unicorn.pid ]")
            warn " unicorn is already RUNNING. "
          else
            execute :unicorn, "-c #{deploy_to}/current/config/unicorn.conf.rb -D"
          end
        end
      end
    end
  end

  desc "stop unicorn"
  task :stop do
    on roles(:app) do
      with rails_env: fetch(:rails_env) do
        within "#{deploy_to}/current" do
          if test("[ -f #{deploy_to}/current/tmp/pids/unicorn.pid ]")
            execute "kill -QUIT `cat #{deploy_to}/current/tmp/pids/unicorn.pid`"
          else
            warn " NO unicorn instances found."
          end
        end
     end
    end
  end

  desc "restart unicorn"
  task :restart do
    on roles(:app) do
      with rails_env: fetch(:rails_env) do
        within "#{deploy_to}/current" do
          if test("[ -f #{deploy_to}/current/tmp/pids/unicorn.pid ]")
            execute "kill -USR2 `cat #{deploy_to}/current/tmp/pids/unicorn.pid`"
          else
            invoke "unicorn:start"
          end
        end
      end
    end
  end

  desc "status unicorn"
  task :status do
    on roles(:app) do
      with rails_env: fetch(:rails_env) do
        within "#{deploy_to}/current" do
          info "show unicorn status"
          execute "cd #{fetch(:deploy_to)}/current/ && pstree `cat tmp/pids/unicorn.pid` -p"
        end
      end
    end
  end

end

namespace :deploy do

  desc "start servers"
  task :start do
    %w[nginx redis unicorn delayed_job].each do |service|
      invoke "#{service}:start", raise_on_non_zero_exit: false
    end
  end

  desc "stop servers"
  task :stop do
    %w[nginx redis unicorn delayed_job].each do |service|
      invoke "#{service}:stop", raise_on_non_zero_exit: false
    end
  end

  desc "restart servers"
  task :restart do
    invoke "unicorn:restart"
    invoke "delayed_job:restart"
  end

  after :publishing, 'deploy:restart'
  after :finishing, 'deploy:cleanup'
end

namespace :log do
  desc "tail rails logs"
  task :rails do
    on roles(:app) do
      with rails_env: fetch(:rails_env) do
        within "#{deploy_to}/current" do
          execute "cd #{deploy_to}/current/; tail -f log/#{fetch(:rails_env)}.log"
        end
      end
    end
  end

  
  %w[unicorn.stderr uncorn.stdout].each do |log_type|
    desc "tail #{log_type} logs"
    task log_type do
      on roles(:app) do
        with rails_env: fetch(:rails_env) do
          within "#{deploy_to}/current" do
            execute "cd #{deploy_to}/current/; tail -f log/#{log_type}.log"
          end
        end
      end
    end
  end
end

namespace :uploads do
  desc "info for uploads"
  task :info do
    on roles(:app) do
      if test("[ -d #{deploy_to}/shared/public/uploads ]")
        execute "du -h -s #{deploy_to}/shared/public/uploads"
      end
    end
  end

  desc "clean upload files"
  task :clean do
    on roles(:app) do
      with rails_env: fetch(:rails_env) do
        if test("[ -d #{deploy_to}/shared/public/uploads ]")
          execute "rm -rf #{deploy_to}/shared/public/uploads/*"
        end
      end
    end
  end
end

namespace :info do
  desc "show the cwd of uncicorn"
  task :unicorn_cwd do
    on roles(:app) do
      if test("[ -f #{deploy_to}/current/tmp/pids/unicorn.pid ]")
        execute "cd #{deploy_to}/current && /usr/sbin/lsof -p `cat tmp/pids/unicorn.pid`|grep 'cwd'"
      else
        info "unicorn NOT running."
      end
    end
  end

  task :revisions do
    on roles(:app) do
      execute "cd #{deploy_to} && tail -n3 ./revisions.log"
    end
  end

end

task :infos do
  on roles(:all) do |host|
    info "Show infos on: #{host.hostname}, deploy to: #{deploy_to}"
    execute :env
    execute 'free'
    execute 'df -h'
    invoke "uploads:info", raise_on_non_zero_exit: false
    execute 'uptime'

    %w[nginx redis unicorn mysqld].each do |service|
      invoke "#{service}:status", raise_on_non_zero_exit: false
    end

    execute "cd #{deploy_to}/current && cat REVISION"
    invoke "info:revisions", raise_on_non_zero_exit: false
    invoke "info:unicorn_cwd", raise_on_non_zero_exit: false

  end
end


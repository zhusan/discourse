require_relative "../../config/environment"
require_relative "../../lib/spam_classifier"

module Imports; end

class Imports::Base

  def initialize
    puts "Starting the import at: #{get_start_time(:import)}"
    @import_date = "timestamp '#{get_start_time(:import)}'"
    @spam_classifier = SpamClassifier.new
    @classifier_was_summarized = false
    silence_logs
    preload_i18n
  end

  def perform
    disable_rate_limiter
    change_site_settings

    load_imported_groups
    load_imported_users
    load_imported_categories
    load_imported_topics
    load_imported_posts

    # in-memory indexes to speed-up username checks
    load_existing_emails
    load_existing_usernames
    # in-memory index of post numbers
    load_existing_post_numbers

    execute

    fix_posts
    fix_topics
    fix_users
    fix_groups
    fix_categories

    elapsed = Time.now - get_start_time(:import)
    puts "\n\nDone (%02dh %02dmin %02dsec)" % [elapsed / 3600, elapsed / 60 % 60, elapsed % 60]
  ensure
    reset_site_settings
    enable_rate_limiter
  end

  def execute
    # must be implemented by importers
    raise NotImplementedError
  end

  def batches(batch_size=5000)
    offset = 0
    loop do
      yield(batch_size, offset)
      offset += batch_size
    end
  end

  def train(spam_or_ham, document)
    @spam_classifier.train(spam_or_ham, document)
  end

  def summarize!
    @spam_classifier.summarize!
    @classifier_was_summarized = true
  end

  def is_spam?(document)
    return false unless @classifier_was_summarized
    @spam_classifier.is_spam?(document)
  end

  def create_users(results, total, offset)
    start_time = get_start_time(:users)
    users = []

    results.each do |user|
      u = yield(user)

      if u.present? && u[:id].present? && u[:email].present?
        u[:email] = u[:email].strip.downcase

        # merge users based on email
        if @email_to_user_id.has_key?(u[:email].to_s)
          @imported_to_new_user_id[u[:id].to_s] = @email_to_user_id[u[:email].to_s]
          next
        end

        u[:username] = suggest_username(u[:username], u[:name], u[:email])
        @existing_usernames << u[:username].downcase

        @imported_user_id_to_email[u[:id].to_s] = u[:email]
        @imported_user_id_to_username[u[:id].to_s] = u[:username]

        u[:name] = User.suggest_name(u[:email]) if u[:name].blank?
        u[:active] ||= true
        u[:email_digests] ||= true
        u[:external_links_in_new_tab] ||= SiteSetting.default_other_external_links_in_new_tab
        u[:trust_level] ||= TrustLevel[1]
        u[:created_at] ||= get_start_time(:import)
        u[:updated_at] ||= get_start_time(:import)
        u[:last_emailed_at] ||= get_start_time(:import)

        users << u
      end
    end

    if users.size > 0

      values = users.map do |user|
        v = [
          "'#{user[:email]}'",
          "'#{user[:username]}'",
          "'#{user[:username].downcase}'",
          "'#{user[:name].gsub("'", "''")}'",
          (user[:active] ? "TRUE" : "FALSE"),
          (user[:email_digests] ? "TRUE" : "FALSE"),
          (user[:admin] ? "TRUE" : "FALSE"),
          (user[:moderator] ? "TRUE" : "FALSE"),
          (user[:external_links_in_new_tab] ? "TRUE" : "FALSE"),
          user[:trust_level],
          (user[:date_of_birth].blank? ? "NULL" : "'#{user[:date_of_birth]}'"),
          "timestamp '#{user[:created_at]}'",
          "timestamp '#{user[:updated_at]}'",
          (user[:last_seen_at].blank? ? "NULL" : "timestamp '#{user[:last_seen_at]}'"),
          "timestamp '#{user[:last_emailed_at]}'",
          (user[:ip_address].blank? ? "NULL" : "inet '#{user[:ip_address]}'"),
          (user[:registration_ip_address].blank? ? "NULL" : "inet '#{user[:registration_ip_address]}'"),
        ]
        "(#{v.join(",")})"
      end

      sql = <<-SQL
        INSERT INTO users (email, username, username_lower, name, active, email_digests, admin, moderator, external_links_in_new_tab, trust_level, date_of_birth, created_at, updated_at, last_seen_at, last_emailed_at, ip_address, registration_ip_address)
        VALUES #{values.join(",")}
        RETURNING id;
      SQL

      exec_sql(sql).each_with_index do |d, index|
        user = users[index]
        @imported_to_new_user_id[user[:id].to_s] = d["id"]
        @email_to_user_id[user[:email].to_s] = d["id"]
      end

      # user_custom_fields
      values = users.map do |user|
        v = [
          imported_to_new_user_id(user[:id]),
          IMPORT_ID_CUSTOM_FIELD_SQL,
          "'#{user[:id]}'",
          @import_date, # created_at
          @import_date, # updated_at
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO user_custom_fields (user_id, name, value, created_at, updated_at)
        VALUES #{values.join(",")};
      SQL

      # user_profiles
      values = users.map do |user|
        bio = (user[:bio_raw].blank? ? "NULL" : "'#{user[:bio_raw].gsub("'", "''")}'")
        v = [
          imported_to_new_user_id(user[:id]),
          (user[:location].blank? ? "NULL" : "'#{user[:location].gsub("'", "''")}'"),
          (user[:website].blank? ? "NULL" : "'#{user[:website].gsub("'", "''")}'"),
          bio, # bio_raw
          bio, # bio_cooked
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO user_profiles (user_id, location, website, bio_raw, bio_cooked)
        VALUES #{values.join(",")};
      SQL

      # user_search_data
      locale = "'#{SiteSetting.default_locale}'"
      values = users.map do |user|
        data = (user[:username] << " " << user[:name]).gsub("'", "''").downcase
        v = [
          imported_to_new_user_id(user[:id]),
          locale,
          "'#{data}'",
          "TO_TSVECTOR('simple', '#{data}')",
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO user_search_data (user_id, locale, raw_data, search_data)
        VALUES #{values.join(",")};
      SQL
    end

    print_status(results.size + offset, total, start_time)

  rescue => e
    puts "", "-" * 100
    puts e.message
    puts e.backtrace.join("\n")
  end

  def create_topics(results, total, offset)
    start_time = get_start_time(:topics)
    topics = []

    results.each do |topic|
      t = yield(topic)

      if t.present? &&
         t[:id].present? &&
         t[:title].present? &&
         t[:raw].present? &&
         !@imported_to_new_topic_id.has_key?(t[:id].to_s)

        t[:views] ||= 0
        t[:created_at] ||= get_start_time(:import)
        t[:updated_at] ||= get_start_time(:import)
        t[:fancy_title] = Topic.fancy_title(t[:title])
        t[:slug] = Slug.for(t[:slug].presence || t[:title])
        t[:deleted_by_id] = nil if t[:deleted_at].blank?
        t[:closed] ||= false
        t[:archived] ||= false
        t[:wiki] ||= false

        document = [
          @imported_user_id_to_username[t[:user_id].to_s],
          @imported_user_id_to_email[t[:user_id].to_s],
          t[:ip],
          t[:title],
          t[:raw],
        ].compact.join("\n")

        topics << t unless is_spam?(document)
      end
    end

    if topics.size > 0
      # topics
      values = topics.map do |topic|
        v = [
          imported_to_new_user_id(topic[:user_id]),
          topic[:new_category_id] || imported_to_new_category_id(topic[:category_id]),
          "timestamp '#{topic[:created_at]}'",
          "timestamp '#{topic[:updated_at]}'",
          "timestamp '#{topic[:updated_at]}'", # bumped_at
          "'#{topic[:title].gsub("'", "''")[0...255]}'",
          "'#{topic[:fancy_title].gsub("'", "''")}'",
          "'#{topic[:slug].gsub("'", "''")[0...255]}'",
          topic[:views],
          imported_to_new_user_id(topic[:user_id]), # last_post_user_id
          (topic[:deleted_at].blank? ? "NULL" : "timestamp '#{topic[:deleted_at]}'"),
          (topic[:deteled_by_id].blank? ? "NULL" : imported_to_new_user_id(topic[:deleted_by_id])),
          (topic[:closed] ? "TRUE" : "FALSE"),
          (topic[:archived] ? "TRUE" : "FALSE"),
        ]
        "(#{v.join(",")})"
      end

      sql = <<-SQL
        INSERT INTO topics (user_id, category_id, created_at, updated_at, bumped_at, title, fancy_title, slug, views, last_post_user_id, deleted_at, deleted_by_id, closed, archived)
        VALUES #{values.join(",")}
        RETURNING id;
      SQL

      exec_sql(sql).each_with_index do |d, index|
        @imported_to_new_topic_id[topics[index][:id].to_s] = d["id"]
      end

      # topic_custom_fields (default)
      values = topics.map do |topic|
        v = [
          imported_to_new_topic_id(topic[:id]),
          IMPORT_ID_CUSTOM_FIELD_SQL,
          "'#{topic[:id]}'",
          @import_date, # created_at
          @import_date, # updated_at
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO topic_custom_fields (topic_id, name, value, created_at, updated_at)
        VALUES #{values.join(",")};
      SQL

      # topic_custom_fields (user-provided)
      values = topics.select do |topic|
        topic[:custom_fields].present? &&
        topic[:custom_fields].keys.size > 0
      end.flat_map do |topic|
        topic[:custom_fields].keys.map do |key|
          v = [
            imported_to_new_topic_id(topic[:id]),
            "'#{key}'",
            "'#{topic[:custom_fields][key].gsub("'", "''")}'",
            @import_date, # created_at
            @import_date, # updated_at
          ]
          "(#{v.join(",")})"
        end
      end

      if values.size > 0
        exec_sql <<-SQL
          INSERT INTO topic_custom_fields (topic_id, name, value, created_at, updated_at)
          VALUES #{values.join(",")};
        SQL
      end

      # topic_search_data
      locale = "'#{SiteSetting.default_locale}'"
      values = topics.map do |topic|
        data = topic[:title] << " " << SearchObserver.scrub_html_for_search(topic[:raw])[0...Topic::MAX_SIMILAR_BODY_LENGTH]
        data.gsub!("'", "''")
        v = [
          imported_to_new_topic_id(topic[:id]),
          locale,
          "'#{data}'",
          "TO_TSVECTOR('#{Search.long_locale}', '#{data}')",
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO topic_search_data (topic_id, locale, raw_data, search_data)
        VALUES #{values.join(",")};
      SQL

      # topic_views
      values = topics.select do |topic|
        topic[:ip].present?
      end.map do |topic|
        v = [
          imported_to_new_user_id(topic[:user_id]),
          imported_to_new_topic_id(topic[:id]),
          "date '#{topic[:created_at]}'", # viewed_at
          "inet '#{topic[:ip]}'",
        ]
        "(#{v.join(",")})"
      end

      if values.size > 0
        exec_sql <<-SQL
          INSERT INTO topic_views (user_id, topic_id, viewed_at, ip_address)
          VALUES #{values.join(",")};
        SQL
      end

      # posts
      values = topics.map do |topic|
        v = [
          imported_to_new_user_id(topic[:user_id]),
          imported_to_new_topic_id(topic[:id]),
          1, # post_number
          1, # sort_order
          1, # reads
          "'#{topic[:raw].gsub("'", "''")}'",
          "'#{topic[:raw].gsub("'", "''")}'", # cooked
          "timestamp '#{topic[:created_at]}'",
          "timestamp '#{topic[:updated_at]}'",
          "timestamp '#{topic[:updated_at]}'", # last_version_at
          (topic[:deleted_at].blank? ? "NULL" : "timestamp '#{topic[:deleted_at]}'"),
          (topic[:deteled_by_id].blank? ? "NULL" : imported_to_new_user_id(topic[:deleted_by_id])),
          word_count(topic[:raw]),
        ]
        "(#{v.join(",")})"
      end

      sql = <<-SQL
        INSERT INTO posts (user_id, topic_id, post_number, sort_order, reads, raw, cooked, created_at, updated_at, last_version_at, deleted_at, deleted_by_id, word_count)
        VALUES #{values.join(",")}
        RETURNING id;
      SQL

      exec_sql(sql).each_with_index do |d, index|
        @imported_to_new_post_id[topics[index][:id].to_s] = d["id"]
      end

      # post_custom_fields
      values = topics.map do |topic|
        v = [
          imported_to_new_post_id(topic[:id]),
          IMPORT_ID_CUSTOM_FIELD_SQL,
          "'#{topic[:id]}'",
          @import_date, # created_at
          @import_date, # updated_at
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO post_custom_fields (post_id, name, value, created_at, updated_at)
        VALUES #{values.join(",")};
      SQL

      # post_search_data
      values = topics.map do |topic|
        data = SearchObserver.scrub_html_for_search(topic[:raw])
        data << " " << topic[:title]
        data.gsub!("'", "''")
        # TODO: add category
        v = [
          imported_to_new_post_id(topic[:id]),
          locale,
          "'#{data}'",
          "TO_TSVECTOR('#{Search.long_locale}', '#{data}')",
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO post_search_data (post_id, locale, raw_data, search_data)
        VALUES #{values.join(",")};
      SQL

    end

    print_status(results.size + offset, total, start_time)

  rescue => e
    puts "", "-" * 100
    puts e.message
    puts e.backtrace.join("\n")
  end

  def create_posts(results, total, offset)
    start_time = get_start_time(:posts)
    posts = []

    results.each do |post|
      p = yield(post)

      if p.present? &&
         p[:id].present? &&
         p[:topic_id].present? &&
         p[:raw].present? &&
         !@imported_to_new_post_id.has_key?(p[:id].to_s) &&
         @imported_to_new_topic_id.has_key?(p[:topic_id].to_s)
        p[:created_at] ||= get_start_time(:import)
        p[:updated_at] ||= get_start_time(:import)
        p[:deleted_by_id] = nil if p[:deleted_at].blank?
        p[:wiki] ||= false

        document = [
          @imported_user_id_to_username[p[:user_id].to_s],
          @imported_user_id_to_email[p[:user_id].to_s],
          p[:ip],
          p[:raw],
        ].compact.join("\n")

        posts << p unless is_spam?(document)
      end
    end

    if posts.size > 0
      # posts
      values = posts.map do |post|
        new_topic_id = imported_to_new_topic_id(post[:topic_id])
        reply_to_post_number = imported_post_id_to_post_number(post[:reply_to_post_id])
        post_number = next_post_number(new_topic_id)
        v = [
          imported_to_new_user_id(post[:user_id]),
          new_topic_id,
          post_number,
          post_number, # sort_order
          1, # reads
          "'#{post[:raw].gsub("'", "''")}'",
          "'#{post[:raw].gsub("'", "''")}'", # cooked
          "timestamp '#{post[:created_at]}'",
          "timestamp '#{post[:updated_at]}'",
          "timestamp '#{post[:updated_at]}'", # last_version_at
          (post[:deleted_at].blank? ? "NULL" : "timestamp '#{post[:deleted_at]}'"),
          (post[:deteled_by_id].blank? ? "NULL" : imported_to_new_user_id(post[:deleted_by_id])),
          (reply_to_post_number.nil? ? "NULL" : reply_to_post_number),
          word_count(post[:raw]),
        ]
        "(#{v.join(",")})"
      end

      sql = <<-SQL
        INSERT INTO posts (user_id, topic_id, post_number, sort_order, reads, raw, cooked, created_at, updated_at, last_version_at, deleted_at, deleted_by_id, reply_to_post_number, word_count)
        VALUES #{values.join(",")}
        RETURNING id, post_number;
      SQL

      exec_sql(sql).each_with_index do |d, index|
        imported_id = posts[index][:id].to_s
        @imported_to_new_post_id[imported_id.to_s] = d["id"]
        @imported_post_id_to_post_number[imported_id.to_s] = d["post_number"]
      end

      # post_custom_field
      values = posts.map do |post|
        v = [
          imported_to_new_post_id(post[:id]),
          IMPORT_ID_CUSTOM_FIELD_SQL,
          "'#{post[:id]}'",
          @import_date, # created_at
          @import_date, # updated_at
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO post_custom_fields (post_id, name, value, created_at, updated_at)
        VALUES #{values.join(",")};
      SQL

      # post_search_data
      locale = "'#{SiteSetting.default_locale}'"
      values = posts.map do |post|
        data = SearchObserver.scrub_html_for_search(post[:raw])
        # TODO: add title
        # TODO: add category
        data.gsub!("'", "''")
        v = [
          imported_to_new_post_id(post[:id]),
          locale,
          "'#{data}'",
          "TO_TSVECTOR('#{Search.long_locale}', '#{data}')",
        ]
        "(#{v.join(",")})"
      end

      exec_sql <<-SQL
        INSERT INTO post_search_data (post_id, locale, raw_data, search_data)
        VALUES #{values.join(",")};
      SQL

      # topic_views
      index = Set.new
      values = posts.select do |post|
        user_id = imported_to_new_user_id(post[:user_id])
        topic_id = imported_to_new_topic_id(post[:topic_id])
        key = "#{user_id}-#{topic_id}"
        next if index.include?(key)
        index << key

        post[:ip].present?
      end.map do |post|
        v = [
          imported_to_new_user_id(post[:user_id]),
          imported_to_new_topic_id(post[:topic_id]),
          "date '#{post[:created_at]}'",
          "inet '#{post[:ip]}'",
        ]
        "(#{v.join(",")})"
      end

      if values.size > 0
        exec_sql <<-SQL
          WITH to_be_inserted AS (
            SELECT *
            FROM (
              VALUES #{values.join(",")}
            ) AS tbi (user_id, topic_id, viewed_at, ip_address)
          )
          INSERT INTO topic_views (user_id, topic_id, viewed_at, ip_address)
          SELECT user_id, topic_id, viewed_at, ip_address
          FROM to_be_inserted
          WHERE NOT EXISTS (
            SELECT 1
              FROM topic_views
             WHERE topic_views.user_id = to_be_inserted.user_id
               AND topic_views.topic_id = to_be_inserted.topic_id
          );
        SQL
      end

    end

    print_status(results.size + offset, total, start_time)

  rescue => e
    puts "", "-" * 100
    puts e.message
    puts e.backtrace.join("\n")
    p posts.map { |p| p.select { |k, _| k.to_s[/^id|topic_id|user_id$/] } }
  end

  private

    def silence_logs
      # set level to "errors" so that we don't create log files that are many GB
      Rails.logger.level = 3
    end

    def preload_i18n
      I18n.t("test")
      ActiveSupport::Inflector.transliterate("test")
    end

    def disable_rate_limiter
      RateLimiter.disable
    end

    def enable_rate_limiter
      RateLimiter.enable
    end

    def import_site_settings
      @import_site_settings ||= {
        disable_emails: true,
      }
    end

    def change_site_settings
      @site_settings = {}

      import_site_settings.each do |k, v|
        @site_settings[k] = SiteSetting.send(k)
        SiteSetting.set(k, v)
      end
    end

    def reset_site_settings
      @site_settings.each do |k, v|
        current = SiteSetting.send(k)
        SiteSetting.set(k, v) unless current != @import_site_settings[k]
      end
    end

    def exec_sql(sql)
      @connection ||= ActiveRecord::Base.connection.raw_connection
      @connection.exec(sql)
    end

    def load_existing_emails
      @email_to_user_id = {}
      User.unscoped.pluck(:email, :id).each do |email, id|
        @email_to_user_id[email.to_s] = id
      end
    end

    def load_existing_usernames
      @existing_usernames = User.unscoped.pluck(:username_lower).to_set
      @existing_usernames.merge(SiteSetting.reserved_usernames.split("|"))
    end

    def suggest_username(username, name, email)
      suggested_username = username

      if is_username_invalid?(suggested_username)
        suggested_username = UserNameSuggester.sanitize_username(username)
        if is_username_invalid?(suggested_username)
          suggested_username = UserNameSuggester.sanitize_username(name)
          if is_username_invalid?(suggested_username)
            suggested_username = UserNameSuggester.sanitize_username(UserNameSuggester.parse_name_from_email(email))
          end
        end
      end

      i = 1
      username = suggested_username

      while @existing_usernames.include?(username.downcase)
        suffix = i.to_s
        max_length = SiteSetting.max_username_length - suffix.size - 1
        username = "#{suggested_username[0..max_length]}#{suffix}"
        i += 1
      end

      username
    end

    def is_username_invalid?(username)
      username.blank? ||
      username.size < SiteSetting.min_username_length ||
      username.size > SiteSetting.max_username_length ||
      username[/[^\w.-]/] ||
      username[0][/\W/] ||
      username[-1][/\W/] ||
      username[/[_.-]{2,}/] ||
      username[/\.(css|js|json|html?|xml|bmp|gif|jpe?g|png|tiff?|woff)$/i]
    end

    def load_existing_post_numbers
      @post_numbers = Hash.new(1)

      exec_sql(<<-SQL
        SELECT topic_id, MAX(post_number) AS max_post_number
          FROM posts
         WHERE deleted_at IS NULL
      GROUP BY topic_id
      SQL
      ).each { |d| @post_numbers[d["topic_id"].to_s] = d["max_post_number"].to_i }
    end

    def next_post_number(new_topic_id)
      @post_numbers[new_topic_id.to_s] += 1
    end

    IMPORT_ID_CUSTOM_FIELD = "import_id".freeze
    IMPORT_ID_CUSTOM_FIELD_SQL = "'#{IMPORT_ID_CUSTOM_FIELD}'".freeze

    def load_imported_groups
      puts "Loading imported groups..."
      @imported_to_new_group_id = {}
      GroupCustomField.where(name: IMPORT_ID_CUSTOM_FIELD)
                      .pluck(:value, :group_id)
                      .each do |imported_id, new_id|
        @imported_to_new_group_id[imported_id.to_s] = new_id
      end
    end

    def load_imported_users
      puts "Loading imported users..."
      @imported_to_new_user_id = {}
      @imported_user_id_to_email = {}
      @imported_user_id_to_username = {}
      UserCustomField.where(name: IMPORT_ID_CUSTOM_FIELD)
                     .joins(:user)
                     .pluck("user_custom_fields.value, users.id, users.email, users.username_lower")
                     .each do |imported_id, new_id, email, username|
        @imported_to_new_user_id[imported_id.to_s] = new_id
        @imported_user_id_to_email[imported_id.to_s] = email
        @imported_user_id_to_username[imported_id.to_s] = username
      end
    end

    def load_imported_categories
      puts "Loading imported categories..."
      @imported_to_new_category_id = {}
      CategoryCustomField.where(name: IMPORT_ID_CUSTOM_FIELD)
                         .pluck(:value, :category_id)
                         .each do |imported_id, new_id|
        @imported_to_new_category_id[imported_id.to_s] = new_id
      end
    end

    def load_imported_topics
      puts "Loading imported topics..."
      @imported_to_new_topic_id = {}
      TopicCustomField.where(name: IMPORT_ID_CUSTOM_FIELD)
                      .pluck(:value, :topic_id)
                      .each do |imported_id, new_id|
        @imported_to_new_topic_id[imported_id.to_s] = new_id
      end
    end

    def load_imported_posts
      puts "Loading imported posts..."
      @imported_to_new_post_id = {}
      @imported_post_id_to_post_number = {}
      PostCustomField.where(name: IMPORT_ID_CUSTOM_FIELD)
                     .joins(:post)
                     .pluck(:value, :post_id, :post_number)
                     .each do |imported_id, new_id, post_number|
        @imported_to_new_post_id[imported_id.to_s] = new_id
        @imported_post_id_to_post_number[imported_id.to_s] = post_number
      end
    end

    def imported_to_new_group_id(imported_group_id)
      @imported_to_new_group_id[imported_group_id.to_s]
    end

    def imported_to_new_user_id(imported_user_id)
      @imported_to_new_user_id[imported_user_id.to_s] || Discourse::SYSTEM_USER_ID
    end

    def imported_to_new_category_id(imported_category_id)
      @imported_to_new_category_id[imported_category_id.to_s] || SiteSetting.uncategorized_category_id
    end

    def imported_to_new_topic_id(imported_topic_id)
      @imported_to_new_topic_id[imported_topic_id.to_s]
    end

    def imported_to_new_post_id(imported_post_id)
      @imported_to_new_post_id[imported_post_id.to_s]
    end

    def imported_post_id_to_post_number(imported_post_id)
      @imported_post_id_to_post_number[imported_post_id.to_s]
    end

    def word_count(text)
      (text || "").scan(/\w+/).size
    end

    def fix_posts
      puts "", "Fixing posts..."

      # TODO: post_revisions

      puts "Creating `post_timings` records..."
      exec_sql <<-SQL
        WITH pt AS (
            SELECT topic_id
                 , post_number
                 , user_id
                 , 5000 AS msecs
              FROM posts
             WHERE user_id <> -1
               AND deleted_at IS NULL
          GROUP BY topic_id, post_number, user_id
        )
        INSERT INTO post_timings (topic_id, post_number, user_id, msecs)
        SELECT *
        FROM pt
        WHERE NOT EXISTS (
          SELECT 1
            FROM post_timings pt1
           WHERE pt1.topic_id = pt.topic_id
             AND pt1.user_id = pt.user_id
             AND pt1.post_number = pt.post_number
        )
      SQL

      puts "Creating `user_actions` records for NEW_TOPIC/REPLY..."
      exec_sql <<-SQL
        WITH ua AS (
            SELECT CASE post_number WHEN 1 THEN 4 ELSE 5 END AS action_type
                 , topic_id AS target_topic_id
                 , id AS target_post_id
                 , user_id
                 , user_id AS acting_user_id
                 , created_at
                 , updated_at
              FROM posts
             WHERE user_id IS NOT NULL
               AND deleted_at IS NULL
          GROUP BY topic_id, user_id, id
        )
        INSERT INTO user_actions (action_type, target_topic_id, target_post_id, user_id, acting_user_id, created_at, updated_at)
        SELECT *
        FROM ua
        WHERE NOT EXISTS (
          SELECT 1
            FROM user_actions ua1
           WHERE ua1.action_type = ua.action_type
             AND ua1.user_id = ua.user_id
             AND ua1.target_topic_id = ua.target_topic_id
             AND ua1.target_post_id = ua.target_post_id
             AND ua1.acting_user_id = ua.acting_user_id
        )
      SQL

      puts "Creating `post_replies` records..."
      exec_sql <<-SQL
        WITH pr AS (
            SELECT id AS post_id
                 , (SELECT id FROM posts p WHERE posts.topic_id = p.topic_id AND posts.reply_to_post_number = p.post_number AND p.deleted_at IS NULL) AS reply_id
                 , created_at
                 , updated_at
              FROM posts
             WHERE user_id IS NOT NULL
               AND deleted_at IS NULL
               AND reply_to_post_number IS NOT NULL
          ORDER BY post_id, reply_id
        )
        INSERT INTO post_replies (post_id, reply_id, created_at, updated_at)
        SELECT *
          FROM pr
         WHERE NOT EXISTS (
            SELECT 1
              FROM post_replies pr1
             WHERE pr1.post_id = pr.post_id
               AND pr1.reply_id = pr.reply_id
        )
      SQL

      puts "Fixing `posts.reply_count`..."
      exec_sql <<-SQL
        UPDATE posts
           SET reply_count = rc.reply_count
          FROM (SELECT post_id, COUNT(*) AS reply_count FROM post_replies GROUP BY post_id) rc
         WHERE posts.id = rc.post_id
           AND posts.reply_count <> rc.reply_count
      SQL

      puts "Fixing `posts.reply_to_user_id`..."
      exec_sql <<-SQL
        UPDATE posts
           SET reply_to_user_id =  (SELECT user_id FROM posts p WHERE posts.topic_id = p.topic_id AND posts.reply_to_post_number = p.post_number AND p.deleted_at IS NULL)
         WHERE reply_to_user_id <> (SELECT user_id FROM posts p WHERE posts.topic_id = p.topic_id AND posts.reply_to_post_number = p.post_number AND p.deleted_at IS NULL)
           AND reply_to_post_number IS NOT NULL
      SQL

      puts "Calculating Post average time..."
      Post.calculate_avg_time

    rescue => e
      puts "", "-" * 100
      puts e.message
      puts e.backtrace.join("\n")
    end

    def fix_topics
      puts "", "Fixing topics..."

      puts "Creating `topic_users` records..."
      exec_sql <<-SQL
        WITH tu AS (
            SELECT topic_id
                 , user_id
                 , TRUE AS posted
                 , 3 AS notification_level
                 , 5000 * COUNT(*) AS total_msecs_viewed
                 , MAX(post_number) AS last_read_post_number
                 , MAX(post_number) AS highest_seen_post_number
                 , MAX(post_number) AS last_emailed_post_number
                 , MIN(created_at) AS first_visited_at
                 , MAX(created_at) AS last_visited_at
              FROM posts
             WHERE user_id <> 1
               AND deleted_at IS NULL
          GROUP BY topic_id, user_id
        )
        INSERT INTO topic_users (topic_id, user_id, posted, notification_level, total_msecs_viewed, last_read_post_number, highest_seen_post_number, last_emailed_post_number, first_visited_at, last_visited_at)
        SELECT *
        FROM tu
        WHERE NOT EXISTS (
            SELECT 1
              FROM topic_users tu1
             WHERE tu1.topic_id = tu.topic_id
               AND tu1.user_id = tu.user_id
        )
      SQL

      puts "Fixing `topics.last_posted_at`..."
      exec_sql <<-SQL
        UPDATE topics
           SET last_posted_at =  (SELECT MAX(created_at) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL)
         WHERE last_posted_at <> (SELECT MAX(created_at) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL)
      SQL

      puts "Fixing `topics.last_post_user_id`..."
      exec_sql <<-SQL
        UPDATE topics
           SET last_post_user_id = lpui.last_post_user_id
          FROM (
            SELECT topics.id AS topic_id
                 , posts.user_id AS last_post_user_id
              FROM topics
              JOIN posts ON posts.topic_id = topics.id
             WHERE posts.post_number = (SELECT MAX(post_number) FROM posts p WHERE p.topic_id = posts.topic_id AND p.deleted_at IS NULL)
               AND posts.deleted_at IS NULL
               AND topics.deleted_at IS NULL
          ) lpui
         WHERE topics.id = topic_id
           AND topics.last_post_user_id <> lpui.last_post_user_id
      SQL

      puts "Fixing `topics.highest_post_number`..."
      exec_sql <<-SQL
        UPDATE topics
           SET highest_post_number =  (SELECT COALESCE(MAX(post_number), 0) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL)
         WHERE highest_post_number <> (SELECT COALESCE(MAX(post_number), 0) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL)
      SQL

      puts "Fixing `topics.posts_count`..."
      exec_sql <<-SQL
        UPDATE topics
           SET posts_count =  (SELECT COUNT(*) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL)
         WHERE posts_count <> (SELECT COUNT(*) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL)
      SQL

      puts "Fixing `topics.bumped_at`..."
      exec_sql <<-SQL
        UPDATE topics
           SET bumped_at =  (SELECT MAX(created_at) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL AND posts.post_type = #{Post.types[:regular]})
         WHERE bumped_at <> (SELECT MAX(created_at) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL AND posts.post_type = #{Post.types[:regular]})
      SQL

      puts "Fixing `topics.participant_count`..."
      visible_post_types_sql = Topic.visible_post_types.join(",")
      exec_sql <<-SQL
        UPDATE topics
           SET participant_count =  (SELECT COUNT(DISTINCT user_id) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL AND posts.post_type IN (#{visible_post_types_sql}))
         WHERE participant_count <> (SELECT COUNT(DISTINCT user_id) FROM posts WHERE posts.topic_id = topics.id AND posts.deleted_at IS NULL AND posts.post_type IN (#{visible_post_types_sql}))
      SQL

      puts "Ensuring TopicUser's consistenty..."
      TopicUser.ensure_consistency!

      puts "Ensuring TopicFeaturedUser's consistenty..."
      TopicFeaturedUsers.ensure_consistency!

      puts "Calculating Topic average time..."
      Topic.calculate_avg_time
    rescue => e
      puts "", "-" * 100
      puts e.message
      puts e.backtrace.join("\n")
    end

    def fix_users
      puts "", "Fixing users..."

      # TODO: user_histories (when deleting posts/topics)

      puts "Creating `user_visits` records..."
      exec_sql <<-SQL
        WITH uv AS (
            SELECT user_id
                 , DATE(created_at) AS visited_at
                 , COUNT(id) AS posts_read
              FROM posts
             WHERE user_id <> -1
               AND deleted_at IS NULL
          GROUP BY user_id, DATE(created_at)
        )
        INSERT INTO user_visits (user_id, visited_at, posts_read)
        SELECT *
          FROM uv
         WHERE NOT EXISTS (
            SELECT 1
              FROM user_visits uv1
             WHERE uv1.user_id = uv.user_id
               AND uv1.visited_at = uv.visited_at
        )
      SQL

      puts "Creating `user_stats` records..."
      exec_sql <<-SQL
        WITH us AS (
              SELECT p.user_id
                   , now() AS new_since
                   , (SELECT COUNT(*) FROM user_visits v WHERE v.user_id = p.user_id) AS days_visited
                   , COUNT(DISTINCT(p.topic_id)) AS topics_entered
                   , COUNT(DISTINCT(p.topic_id)) AS topic_count
                   , (5 * COUNT(p.id)) AS time_read
                   , COUNT(p.id) AS posts_read_count
                   , COUNT(p.id) AS post_count
                   , MIN(p.created_at) AS first_post_created_at
              FROM posts p
         LEFT JOIN user_stats us ON us.user_id = p.user_id
             WHERE p.user_id <> -1
               AND p.deleted_at IS NULL
               AND us.user_id IS NULL
          GROUP BY p.user_id
        )
        INSERT INTO user_stats (user_id, new_since, days_visited, topics_entered, topic_count, time_read, posts_read_count, post_count, first_post_created_at)
        SELECT * FROM us
      SQL

      # also take into accounts users who have never posted
      exec_sql <<-SQL
        WITH us AS (
              SELECT u.id AS user_id
                   , now() AS new_since
                   , (SELECT COUNT(*) FROM user_visits v WHERE v.user_id = u.id) AS days_visited
              FROM users u
         LEFT JOIN user_stats us ON us.user_id = u.id
             WHERE u.id <> -1
               AND us.user_id IS NULL
        )
        INSERT INTO user_stats (user_id, new_since, days_visited)
        SELECT * FROM us
      SQL

      puts "Fixing `user_stats.topic_reply_count`..."
      UserStat.pluck(:user_id).each do |user_id|
        UserStat.exec_sql <<-SQL
          WITH trc AS (
            SELECT COUNT(*) AS topic_reply_count
              FROM topics
             WHERE deleted_at IS NULL
               AND id IN (
                   SELECT topic_id
                     FROM posts p
                     JOIN topics t2 ON t2.id = p.topic_id
                    WHERE p.deleted_at IS NULL
                      AND t2.user_id <> p.user_id
                      AND p.user_id = #{user_id}
                 GROUP BY topic_id
               )
          )
          UPDATE user_stats
             SET topic_reply_count =  trc.topic_reply_count
            FROM trc
           WHERE user_stats.topic_reply_count <> trc.topic_reply_count
             AND user_stats.user_id = #{user_id}
        SQL
      end

      puts "Fixing `users.last_posted_at`..."
      exec_sql <<-SQL
        UPDATE users
           SET last_posted_at =  (SELECT MAX(created_at) FROM posts WHERE posts.user_id = users.id AND posts.deleted_at IS NULL)
         WHERE last_posted_at <> (SELECT MAX(created_at) FROM posts WHERE posts.user_id = users.id AND posts.deleted_at IS NULL)
      SQL

      puts "Fixing `users.last_seen_at`..."
      exec_sql <<-SQL
        UPDATE users
           SET last_seen_at =  GREATEST(last_seen_at, (SELECT MAX(created_at) FROM posts WHERE posts.user_id = users.id AND posts.deleted_at IS NULL))
         WHERE last_seen_at <> GREATEST(last_seen_at, (SELECT MAX(created_at) FROM posts WHERE posts.user_id = users.id AND posts.deleted_at IS NULL))
      SQL

    rescue => e
      puts "", "-" * 100
      puts e.message
      puts e.backtrace.join("\n")
    end

    def fix_groups
      puts "", "Fixing groups..."

      puts "Refreshing automatic groups..."
      Group.refresh_automatic_groups!

    rescue => e
      puts "", "-" * 100
      puts e.message
      puts e.backtrace.join("\n")
    end

    def fix_categories
      puts "", "Fixing categories..."

      puts "Updating category stats..."
      Category.update_stats

      puts "Featuring category topics..."
      CategoryFeaturedTopic.feature_topics

    rescue => e
      puts "", "-" * 100
      puts e.message
      puts e.backtrace.join("\n")
    end

    def get_start_time(key)
      @start_times ||= {}
      @start_times[key] ||= Time.now
    end

    def print_status(current, max, start_time)
      elapsed_seconds = Time.now - start_time
      print "\r%9d / %d (%6.2f%%) [%.0f items/min]" % [current, max, current / max.to_f * 100, current / elapsed_seconds.to_f * 60]
    end
end

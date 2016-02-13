require "csv"
require "mysql2"
require_relative "base"

class Imports::TNation < Imports::Base

  CATEGORY_ID_FROM_IMPORTED_CATEGORY_ID = {
     83 => 5,
     23 => 7,
    208 => 8,
    250 => 9,
    234 => 10,
     39 => 11,
     29 => 12,
    229 => 13,
     81 => 14,
     64 => 15,
     62 => 16,
     82 => 17,
    212 => 18,
    210 => 19,
    211 => 20,
    216 => 21,
      3 => 22,
     32 => 23,
      6 => 24,
    217 => 25,
     40 => 26,
  }

  MIGRATED_CATEGORY_IDS = CATEGORY_ID_FROM_IMPORTED_CATEGORY_ID.keys.sort
  MIGRATED_CATEGORY_IDS_SQL = MIGRATED_CATEGORY_IDS.join(",")

  USER_ID_MAPPING_PATH = "/Users/zogstrip/Downloads/forum-biotest-map.csv"
  USERS_MERGED_PATH = "/Users/zogstrip/Downloads/forum-users-merged-8.csv"

  def initialize
    super
  end

  def execute
    # list_imported_user_ids
    load_user_id_mapping(USER_ID_MAPPING_PATH)

    import_users(USERS_MERGED_PATH)
    import_topics
    import_posts
  end

  def list_imported_user_ids
    puts "", "listing imported user_ids..."

    forum_query("DROP TABLE IF EXISTS authors")
    forum_query("CREATE TABLE authors AS (SELECT DISTINCT(author_id) FROM forum_message)")
    forum_query("CREATE UNIQUE INDEX author_id_index ON authors (author_id)")
  end

  def load_user_id_mapping(path)
    puts "", "loading user_id mapping..."

    # id,forum-id
    file = File.read(path).gsub('"', '').gsub("\\N", "")
    user_ids = CSV.parse(file, headers: true)

    @old_to_new = {}

    user_ids.each do |u|
      next if u["forum-id"].blank?
      @old_to_new[u["forum-id"]] = u["id"]
    end

    puts "#{@old_to_new.keys.count} users mapped!"
  end

  def import_users(path)
    puts "", "importing users..."

    users = []
    emails = Set.new

    # id,username,email,first-name,last-name,join-date,location,date-of-birth,website,avatar_path
    file = File.read(path).gsub('"', '').gsub("\\N", "")

    CSV.parse(file, headers: true).each do |u|
      u["email"] = (u["email"] || "").gsub("'", "").strip.downcase.presence
      next if u["email"].blank? || emails.include?(u["email"])
      users << u
      emails << u["email"]
    end

    total = users.size

    batches do |limit, offset|
      user_batch = users[offset..(offset + limit)]
      break if user_batch.nil? || user_batch.empty?
      create_users(user_batch, total, offset) do |user|
        created_at = DateTime.parse(user["join-date"]) rescue nil
        name = "#{user["first-name"]} #{user["last-name"]}".strip.presence
        username = UserNameSuggester.suggest(user["username"] || name || user["email"])
        custom_fields = {}
        custom_fields["mapped_id"] = @old_to_new[user["id"]] if @old_to_new.has_key?(user["id"])
        custom_fields["import_avatar_url"] = forum_avatar_url(user["avatar-path"])

        {
          id: user["id"],
          username: username,
          email: user["email"],
          name: name,
          trust_level: TrustLevel[2],
          created_at: created_at,
          location: user["location"].presence,
          # custom_fields: custom_fields,
        }
      end
    end
  end

  def import_topics
    puts "", "Importing topics..."

    total = forum_query(<<-SQL
      SELECT COUNT(id) AS count
        FROM forum_message
       WHERE author_id IN (SELECT author_id FROM authors)
         AND category_id IN (#{MIGRATED_CATEGORY_IDS_SQL})
         AND status = 1
         AND (edit_parent IS NULL OR edit_parent = -1)
         AND topic_id = id
         AND topic_id > 0
         AND LENGTH(TRIM(subject)) > 0
    SQL
    ).first["count"]

    @last_topic_id = -1

    batches do |limit, offset|
      topics = forum_query(<<-SQL
          SELECT fm.id, fm.category_id, fm.topic_id, fm.date, fm.author_id, TRIM(fm.subject) AS 'subject', fm.message, ft.sticky, tv.views
            FROM forum_message fm
       LEFT JOIN forum_topic ft ON fm.topic_id = ft.id
       LEFT JOIN topic_views tv ON fm.topic_id = tv.topic_id
           WHERE fm.author_id IN (SELECT author_id FROM authors)
             AND fm.category_id IN (#{MIGRATED_CATEGORY_IDS_SQL})
             AND fm.status = 1
             AND (fm.edit_parent IS NULL OR fm.edit_parent = -1)
             AND fm.topic_id = fm.id
             AND fm.topic_id > 0
             AND LENGTH(TRIM(fm.subject)) > 0
             AND fm.id > #{@last_topic_id}
        ORDER BY fm.id
           LIMIT #{limit}
      SQL
      ).to_a

      break if topics.size < 1
      @last_topic_id = topics[-1]["topic_id"]

      # load images
      forum_images = {}
      message_ids_sql = topics.map { |p| p["id"] }.join(",")

      images = forum_query <<-SQL
        SELECT message_id, filename
          FROM forum_image
         WHERE message_id IN (#{message_ids_sql})
           AND width > 0
           AND height > 0
      SQL

      images.each do |image|
        forum_images[image["message_id"]] ||= []
        forum_images[image["message_id"]] << image["filename"]
      end

      create_topics(topics, total, offset) do |topic|
        raw = (topic["message"] || "").gsub("\u0000", "")

        if forum_images.has_key?(topic["id"])
          forum_images[topic["id"]].each do |filename|
            raw = forum_image_url(filename) + "\n\n" + raw
          end
        end

        {
          id: topic["id"],
          user_id: topic["author_id"] || -1,
          new_category_id: CATEGORY_ID_FROM_IMPORTED_CATEGORY_ID[topic["category_id"]],
          title: (topic["subject"] || "").gsub("\u0000", "").presence,
          created_at: topic["date"],
          raw: raw,
          views: topic["views"].presence,
        }
      end
    end
  end

  def import_posts
    puts "", "Importing posts..."

    total = forum_query(<<-SQL
      SELECT COUNT(id) AS count
        FROM forum_message
       WHERE author_id IN (SELECT author_id FROM authors)
         AND category_id IN (#{MIGRATED_CATEGORY_IDS_SQL})
         AND status = 1
         AND (edit_parent IS NULL OR edit_parent = -1)
         AND topic_id <> id
         AND topic_id > 0
    SQL
    ).first["count"]

    @last_post_id = -1

    batches do |limit, offset|
      posts = forum_query(<<-SQL
          SELECT fm.id, fm.topic_id, fm.date, fm.author_id, fm.message
            FROM forum_message fm
       LEFT JOIN forum_topic ft ON fm.topic_id = ft.id
           WHERE fm.author_id IN (SELECT author_id FROM authors)
             AND fm.category_id IN (#{MIGRATED_CATEGORY_IDS_SQL})
             AND fm.status = 1
             AND (fm.edit_parent IS NULL OR fm.edit_parent = -1)
             AND fm.topic_id <> fm.id
             AND fm.topic_id > 0
             AND fm.id > #{@last_post_id}
        ORDER BY fm.id
           LIMIT #{limit}
      SQL
      ).to_a

      break if posts.size < 1
      @last_post_id = posts[-1]["id"]

       # load images
      forum_images = {}
      message_ids_sql = posts.map { |p| p["id"] }.join(",")

      images = forum_query <<-SQL
        SELECT message_id, filename
          FROM forum_image
         WHERE message_id IN (#{message_ids_sql})
           AND width > 0
           AND height > 0
      SQL

      images.each do |image|
        forum_images[image["message_id"]] ||= []
        forum_images[image["message_id"]] << image["filename"]
      end

      create_posts(posts, total, offset) do |post|
        raw = (post["message"] || "").gsub("\u0000", "")

        if forum_images.has_key?(post["id"])
          forum_images[post["id"]].each do |filename|
            raw = forum_image_url(filename) + "\n\n" + raw
          end
        end

        {
          id: post["id"],
          topic_id: post["topic_id"],
          user_id: post["author_id"] || -1,
          created_at: post["date"],
          raw: raw,
        }
      end
    end
  end

  def forum_avatar_url(avatar_path)
    return if avatar_path.blank?
    "http://images.t-nation.com#{avatar_path}"
  end

  def forum_image_url(filename)
    "http://images.t-nation.com/forum_images/#{filename[0]}/#{filename[1]}/#{filename}"
  end

  def forum_query(sql)
    Mysql2::Client.new(username: "root", database: "uberforum").query(sql)
  end

end

Imports::TNation.new.perform

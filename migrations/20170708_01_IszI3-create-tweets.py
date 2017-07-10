"""
create tweets
"""

from yoyo import step

__depends__ = {}

steps = [
    step("CREATE TABLE tweets " \
         "("\
           "id BIGINT, " \
           "created_at TIMESTAMP, " \
           "lang VARCHAR(8), " \
           "user_id BIGINT, " \
           "user_created_at TIMESTAMP, " \
           "user_name TEXT, " \
           "user_screen_name TEXT, " \
           "user_lang VARCHAR(8), " \
           "user_mentions_ids BIGINT[], " \
           "user_mentions_names TEXT[], " \
           "user_mentions_screen_names TEXT[], " \
           "in_reply_to_status_id BIGINT, " \
           "in_reply_to_user_id BIGINT, " \
           "in_reply_to_screen_name TEXT, " \
           "retweet_count BIGINT, " \
           "favorite_count BIGINT, " \
           "followers_count BIGINT, " \
           "friends_count BIGINT, " \
           "hashtags TEXT[], " \
           "urls TEXT[], " \
           "symbols TEXT[], " \
           "media_urls TEXT[], " \
           "text TEXT, " \
           "PRIMARY KEY (id)" \
         ")",
         "DROP TABLE tweets")
]

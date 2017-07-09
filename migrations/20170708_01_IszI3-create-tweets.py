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
           "lang VARCHAR(4), " \
           "user_id BIGINT, " \
           "user_created_at TIMESTAMP, " \
           "user_name TEXT, " \
           "user_mentions_id BIGINT[], " \
           "user_mentions_name TEXT[], " \
           "user_mentions_screen_name TEXT[], " \
           "in_reply_to_status_id BIGINT, " \
           "in_reply_to_user_id BIGINT, " \
           "in_reply_to_screen_name TEXT, " \
           "retweet_count BIGINT, " \
           "favorite_count BIGINT, " \
           "followers_count BIGINT, " \
           "hashtags TEXT[], " \
           "urls TEXT[], " \
           "media_urls TEXT[], " \
           "text TEXT, " \
           "PRIMARY KEY (id)" \
         ")",
         "DROP TABLE tweets")
]

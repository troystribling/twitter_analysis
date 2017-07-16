from peewee import *
from playhouse.postgres_ext import *

database = PostgresqlDatabase('tweets', **{'user': 'tweets'})

class BaseModel(Model):
    class Meta:
        database = database

class Tweet(BaseModel):
    id = BigIntegerField(primary_key=True, null=False, unique=True)
    created_at = DateTimeTZField()
    favorite_count = BigIntegerField()
    followers_count = BigIntegerField()
    friends_count = BigIntegerField()
    hashtags = ArrayField(TextField)
    in_reply_to_screen_name = TextField()
    in_reply_to_status_id = BigIntegerField()
    in_reply_to_user_id = BigIntegerField()
    lang = CharField()
    media_urls = ArrayField(TextField)
    retweet_count = BigIntegerField()
    statuses_count = BigIntegerField()
    symbols = ArrayField(TextField)
    text = TextField()
    urls = ArrayField(TextField)
    user_created_at = DateTimeTZField()
    user_id = BigIntegerField()
    user_lang = CharField()
    user_mentions_ids = ArrayField(BigIntegerField)
    user_mentions_names = ArrayField(TextField)
    user_mentions_screen_names = ArrayField(TextField)
    user_name = TextField()
    user_screen_name = TextField()

    class Meta:
        db_table = 'tweets'

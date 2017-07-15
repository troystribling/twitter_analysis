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
    followers_count = BigIntegerField(null=True)
    friends_count = BigIntegerField(null=True)
    hashtags = ArrayField(TextField)
    in_reply_to_screen_name = TextField(null=True)
    in_reply_to_status_id = BigIntegerField(null=True)
    in_reply_to_user_id = BigIntegerField(null=True)
    lang = CharField(null=True)
    media_urls = ArrayField(TextField)
    retweet_count = BigIntegerField(null=True)
    statuses_count = BigIntegerField(null=True)
    symbols = ArrayField(TextField)
    text = TextField(null=True)
    urls = ArrayField(TextField)
    user_created_at = DateTimeTZField(null=True)
    user = BigIntegerField(db_column='user_id', null=True)
    user_lang = CharField(null=True)
    user_mentions_ids = ArrayField(BigIntegerField)
    user_mentions_names = ArrayField(TextField)
    user_mentions_screen_names = ArrayField(TextField)
    user_name = TextField(null=True)
    user_screen_name = TextField(null=True)

    class Meta:
        db_table = 'tweets'

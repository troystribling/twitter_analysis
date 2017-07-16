import tempfile
import os
import boto3
import json
import psycopg2

from subprocess import call
from datetime import timedelta, date
from dateutil.parser import parse
from .models import Tweet, database

def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + timedelta(n)

def download_from_s3_to_files(bucket, remote_dir, local_dir, download_limit=None, start_date=None, end_date=None):
    if start_date is None:
        start_date = date.today()
    else:
        start_date = parse(start_date)

    if end_date is None:
        end_date = date.today()
    else:
        end_date = parse(end_date)

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    total_downloaded_file_count = 0
    downloaded_file_count = 0

    for day in daterange(start_date, end_date):
        day_dir = day.strftime('%Y%m%d')
        print(f"DOWNLOADING {day_dir}")
        remote_day_dir = f"{remote_dir}/{day_dir}"
        local_day_dir = f"{local_dir}/{day_dir}"
        if not os.path.exists(local_day_dir):
            os.makedirs(local_day_dir)
        remote_objects = s3_resource.Bucket(bucket).objects.filter(Prefix=remote_day_dir)
        for remote_object in remote_objects:
            total_downloaded_file_count += 1
            downloaded_file_count += 1
            remote_file_name = remote_object.key
            local_file_name = f"{local_day_dir}/{os.path.basename(remote_file_name)}"
            with open(local_file_name, 'wb') as local_file:
                s3_client.download_fileobj(bucket, remote_file_name, local_file)
            call(f'lzop -d {local_file_name}', shell=True)
            os.unlink(local_file_name)
            if download_limit != None and total_downloaded_file_count >= download_limit:
                break
        print(f'DOWNLOADED {downloaded_file_count} files from {remote_day_dir} to {local_day_dir}')
        downloaded_file_count = 0
    print(f'DOWNLOADED {total_downloaded_file_count} files from {remote_dir} to {local_dir}')

def read_from_file(file_name):
    items = []
    with open(file_name, 'r') as file:
        for line in file:
            items.append(json.loads(line))
    return items

def import_tweets_to_database(local_dir, database_cusror):
    date_dirs = os.listdir(local_dir)
    for date_dir in date_dirs:
        for file_path in os.listdir(os.path.join(local_dir, date_dir)):
            items = read_from_file(file_path)
            for item in data_items:
                insert_tweet_sql(flatten_tweet(item), database_cusror)

def array_field_if_exists(tweet, key1, key2, key3):
    value = None
    if key2 in tweet[key1]:
        temp_value = [hashtags[key3] for hashtags in tweet[key1][key2]]
        if len(temp_value) > 0:
            value = temp_value
    return value

def flatten_tweet(tweet):
    return {
            'id' : tweet['id'],
            'created_at' : parse(tweet['created_at']),
            'lang' : tweet['lang'],
            'user_id' : tweet['user']['id'],
            'user_created_at' : parse(tweet['user']['created_at']),
            'user_name' : tweet['user']['name'],
            'user_screen_name' : tweet['user']['screen_name'],
            'user_lang' : tweet['user']['lang'],
            'user_mentions_ids' : array_field_if_exists(tweet, 'entities', 'user_mentions', 'id'),
            'user_mentions_names' : array_field_if_exists(tweet, 'entities', 'user_mentions', 'name'),
            'user_mentions_screen_names' : array_field_if_exists(tweet, 'entities', 'user_mentions', 'screen_name'),
            'in_reply_to_status_id' : tweet['in_reply_to_status_id'],
            'in_reply_to_user_id' : tweet['in_reply_to_user_id'],
            'in_reply_to_screen_name' : tweet['in_reply_to_screen_name'],
            'retweet_count' : tweet['retweet_count'],
            'favorite_count' : tweet['favorite_count'],
            'followers_count' : tweet['user']['followers_count'],
            'friends_count' : tweet['user']['friends_count'],
            'statuses_count' : tweet['user']['statuses_count'],
            'hashtags' : array_field_if_exists(tweet, 'entities', 'hashtags', 'text'),
            'urls' : array_field_if_exists(tweet, 'entities', 'urls', 'expanded_url'),
            'symbols' : array_field_if_exists(tweet, 'entities', 'symbols', 'text'),
            'media_urls' : array_field_if_exists(tweet, 'entities', 'media', 'media_url'),
            'text' : tweet['text']
            }

def create_tweets_from_file(file_path, batch_size=100):
    tweets = read_from_file(file_path)
    flat_tweets = [flatten_tweet(tweet) for tweet in tweets]
    with database.atomic():
        for i in range(0, len(flat_tweets), batch_size):
            Tweet.insert_many(flat_tweets[i:i+batch_size]).execute()

def create_tweets_from_files(local_dir, start_date=None, end_date=None, batch_size=100):
    if start_date is None:
        start_date = date.today()
    else:
        start_date = parse(start_date)
    if end_date is None:
        end_date = date.today()
    else:
        end_date = parse(end_date)

    for day in daterange(start_date, end_date):
        date_dir = day.strftime('%Y%m%d')
        print(f"IMPORTING {date_dir}")
        file_dir = os.path.join(local_dir, date_dir)
        file_names = os.listdir(file_dir)
        for file_name in file_names:
            file_path = os.path.join(file_dir, file_name)
            create_tweets_from_file(file_path, batch_size)

def doanload_from_s3_and_create_tweets(bucket_name, remote_dir, local_dir, start_date=None, end_date=None, batch_size=100):
    download_from_s3_to_files(bucket_name, remote_dir, local_dir, start_date=start_date, end_date=end_date)
    create_tweets_from_files(local_dir, start_date=start_date, end_date=end_date, batch_size=100)

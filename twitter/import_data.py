import tempfile
import os
import boto3
import json
import psycopg2

from subprocess import call
from datetime import timedelta, date
from dateutil.parser import parse
from .models import Tweet

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

    downloaded_file_count = 0

    for day in daterange(start_date, end_date):
        day_dir = day.strftime('%Y%m%d')
        remote_day_dir = f"{remote_dir}/{day_dir}"
        local_day_dir = f"{local_dir}/{day_dir}"
        if not os.path.exists(local_day_dir):
            os.makedirs(local_day_dir)
        remote_objects = s3_resource.Bucket(bucket).objects.filter(Prefix=remote_day_dir)
        for remote_object in remote_objects:
            downloaded_file_count += 1
            remote_file_name = remote_object.key
            local_file_name = f"{local_day_dir}/{os.path.basename(remote_file_name)}"
            with open(local_file_name, 'wb') as local_file:
                s3_client.download_fileobj(bucket, remote_file_name, local_file)
            call(f'lzop -d {local_file_name}', shell=True)
            os.unlink(local_file_name)
            print(downloaded_file_count)
            if download_limit != None and downloaded_file_count >= download_limit:
                break
    print(f'DOWNLOADED {downloaded_file_count} files from {remote_dir} to {local_dir}')

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
    created_at = None
    if 'created_at' in tweet:
        created_at = parse(tweet['created_at'])
    user_created_at = None
    if 'user_created_at' in tweet['user']:
        user_created_at = parse(tweet['user']['created_at'])
    return {
            'id' : tweet['id'],
            'created_at' : created_at,
            'lang' : tweet['lang'],
            'user_id' : tweet['user']['id'],
            'user_created_at' : user_created_at,
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

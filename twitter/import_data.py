import tempfile
import os
import boto3
import json
import psycopg2

from subprocess import call
from datetime import timedelta, date
from dateutil.parser import parse

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

def flatten_tweet(tweet):
    hastags = []
    if 'hashtags' in tweet['entities']:
        hashtags = [hashtag['text'] for harshtag in tweet['entities']['hashtags']]
    media_urls = []
    if 'media' in tweet['entities']:
        media_urls = [media['media_url'] for media in tweet['entities']['media']]
    urls = []
    if 'urls' in tweet['entities']:
        urls = [url['expanded_url'] for url in tweet['entities']['urls']]
    symbols = []
    if 'symbols' in tweet['entities']:
        symbols = [symbol['text'] for symbol in tweet['entities']['symbols']]
    user_mentions_ids = []
    user_mentions_names = []
    user_mentions_screen_names = []
    if 'user_mentions' in tweet['entities']:
        user_mentions_ids = [user_mention['id'] for user_mention in tweet['entities']['user_mentions']]
        user_mentions_names = [user_mention['name'] for user_mention in tweet['entities']['user_mentions']]
        user_mentions_screen_names = [user_mention['screen_name'] for user_mention in tweet['entities']['user_mentions']]
    return {
            'id' : tweet['id'],
            'created_at' : tweet['created_at'],
            'lang' : tweet['lang'],
            'user_id' : tweet['user']['id'],
            'user_created_at' : tweet['user']['created_at'],
            'user_name' : tweet['user']['name'],
            'user_screen_name' : tweet['user']['screen_name'],
            'user_lang' : tweet['user']['lang'],
            'user_mentions_ids' : user_mentions_ids,
            'user_mentions_names' : user_mentions_names,
            'user_mentions_screen_names' : user_mentions_screen_names,
            'in_reply_to_status_id' : tweet['in_reply_to_status_id'],
            'in_reply_to_user_id' : tweet['in_reply_to_user_id'],
            'in_reply_to_screen_name' : tweet['in_reply_to_screen_name'],
            'retweet_count' : tweet['retweet_count'],
            'favorite_count' : tweet['favorite_count'],
            'followers_count' : tweet['user']['followers_count'],
            'friends_count' : tweet['user']['friends_count'],
            'statuses_count' : tweet['user']['statuses_count'],
            'hashtags' : hashtags,
            'urls' : urls,
            'symbols' : symbols,
            'media_urls' : media_urls,
            'text' : tweet['text']
            }

def sql_stringify_array(values):
    return f"'{{{', '.join([str(x) for x in values])}}}'"

def sql_stringify_str_array(values):
    return sql_stringify_array(['"'+x+'"' for x in values])

def insert_tweet_sql(tweet):
    user_mention_ids = f"'{{{', '.join([str(x) for x in tweet['user_mentions_ids']])}}}'"
    user_mentions_names = f"'{{{', '.join(['x' for x in tweet['user_mentions_names']])}}}'"
    return "INSERT INTO tweets" \
             "(" \
                "id, created_at, lang, user_id, user_created_at, user_name, user_screen_name, user_lang, "\
                "user_mentions_ids, user_mentions_names, user_mentions_screen_names, " \
                "in_reply_to_status_id, in_reply_to_user_id, in_reply_to_screen_name, " \
                "retweet_count, favorite_count, followers_count, friends_count, statuses_count, hashtags, urls, " \
                " symbols, media_urls, text" \
             ") " \
           "VALUES " \
             "(" \
               f"{tweet['id']}, " \
               f"'{tweet['created_at']}', " \
               f"'{tweet['lang']}', " \
               f"{tweet['user_id']}, " \
               f"'{tweet['user_created_at']}', " \
               f"'{tweet['user_name']}', " \
               f"'{tweet['user_screen_name']}', " \
               f"'{tweet['user_lang']}', " \
               f"{sql_stringify_array(tweet['user_mentions_ids'])}, " \
               f"{sql_stringify_str_array(tweet['user_mentions_names'])}, " \
               f"{sql_stringify_str_array(tweet['user_mentions_screen_names'])}, " \
               f"{tweet['in_reply_to_status_id']}, " \
               f"{tweet['in_reply_to_user_id']}, " \
               f"'{tweet['in_reply_to_screen_name']}', " \
               f"{tweet['retweet_count']}, " \
               f"{tweet['favorite_count']}, " \
               f"{tweet['followers_count']}, " \
               f"{tweet['friends_count']}, " \
               f"{tweet['statuses_count']}, " \
               f"{sql_stringify_str_array(tweet['hashtags'])}, " \
               f"{sql_stringify_str_array(tweet['urls'])}, " \
               f"{sql_stringify_str_array(tweet['symbols'])}, " \
               f"{sql_stringify_str_array(tweet['media_urls'])}, " \
               f"'{tweet['text']}', " \
             ");"

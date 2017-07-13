# %%
%reload_ext autoreload
%autoreload 2

%aimport tempfile
%aimport os

from twitter import import_data

bucket_name = 'gly.fish'
tempdir = tempfile.gettempdir()

# %%
# fetch twitter collected between dates
remote_dir = 'tweets/public_stream'
local_dir = os.path.join(tempdir, 'tweets/public_stream')
date_dir = '20170707'
download_limit = 5
import_data.download_from_s3_to_files(bucket_name, remote_dir, local_dir, download_limit=download_limit, start_date=date_dir, end_date=date_dir)
files = os.listdir(os.path.join(local_dir, date_dir))
tweets = import_data.read_from_file(os.path.join(local_dir, folder_date, files[0]))
tweet = tweets[909]
tweet['entities']

# %%
# create database models
local_dir = os.path.join(tempdir, 'tweets/public_stream')
date_dir = '20170707'
files = os.listdir(os.path.join(local_dir, date_dir))
tweets = import_data.read_from_file(os.path.join(local_dir, date_dir, files[0]))
tweet = tweets[101]
flat_tweet = import_data.flatten_tweet(tweet)
tweet_insert_sql = import_data.insert_tweet_sql(flat_tweet)

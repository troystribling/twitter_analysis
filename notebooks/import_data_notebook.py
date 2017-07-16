# %%
%reload_ext autoreload
%autoreload 2

%aimport tempfile
%aimport os

from twitter import import_data
from twitter.models import Tweet

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
remote_dir = 'tweets/public_stream'
local_dir = os.path.join(tempdir, 'tweets/public_stream')
start_date = '20170626'
end_date = '20170703'
import_data.download_from_s3_to_files(bucket_name, remote_dir, local_dir, start_date=start_date, end_date=end_date)

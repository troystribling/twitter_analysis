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
download_limit = 20
import_data.download_from_s3_to_files(bucket_name, remote_dir, local_dir, download_limit=download_limit, start_date=date_dir, end_date=date_dir)
files = os.listdir(os.path.join(local_dir, folder_date))
tweets = import_data.read_from_file(os.path.join(local_dir, folder_date, files[0]))
tweet = tweets[9]
tweet['entities']

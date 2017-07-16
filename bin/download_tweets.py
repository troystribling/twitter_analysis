import sys
import os
file_path = os.path.dirname(os.path.join(os.getcwd(), __file__))
sys.path.append(os.path.join(file_path, '..'))

from datetime import timedelta, date
from dateutil.parser import parse
import tempfile

from twitter import import_data

start_date = date.today().strftime('%Y%m%d')
if len(sys.argv) > 1:
    start_date = sys.argv[1]

end_date = start_date
if len(sys.argv) > 2:
    end_date = sys.argv[2]

bucket_name = 'gly.fish'
tempdir = tempfile.gettempdir()
remote_dir = 'tweets/public_stream'
local_dir = os.path.join(tempdir, 'tweets/public_stream')

print(f"DOWNLOADING {start_date} to {end_date}")
print(f"BUCKET: {bucket_name}, REMOTE LOCATION: {remote_dir}, LOCAL LOCATION: {local_dir}")

import_data.download_from_s3_to_files(bucket_name, remote_dir, local_dir, start_date=start_date, end_date=end_date)

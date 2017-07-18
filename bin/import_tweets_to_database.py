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

tempdir = tempfile.gettempdir()
local_dir = os.path.join(tempdir, 'tweets/public_stream')

print(f"IMPORTING {start_date} to {end_date} FROM {local_dir}")

import_data.create_tweets_from_files(local_dir, start_date=start_date, end_date=end_date)

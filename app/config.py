""" Configuration file """
import os
from pytz import timezone
import datetime
from datetime import date, datetime,timedelta


# FILE PATHS #
base_path = os.path.dirname(__file__)
APP_PATH = os.path.join(base_path, "..\\")

# LOGGING #
# SHARED_LOG_FILE_NAME = os.path.join(APP_PATH, "logs\\drop_frequency_optimization.log")
# SHARED_LOGGING_PARAMETERS = {
#     "log_file_name": SHARED_LOG_FILE_NAME,
#     "file_mode": "a",
#     "max_bytes": 5 * 1024 * 1024,
#     "backup_count": 10,
# }

# DATABASE #
SF_DB_ACCOUNT = 'kmartau.ap-southeast-2'
SF_DB_USER = 'da_sf_user'
SF_DB_ROLE = 'KSF_DISCO' #'KSF_DS_DDRPF'
SF_DB_WAREHOUSE = 'KSF_DISCO_WH'  #'KSF_DS_DDRPF_WH' 
SF_DB_DATABASE = 'KSFTA'
SF_DB_SCHEMA = 'DDRPF'
SF_DB_PASSWORD = os.environ["SF_DB_PASSWORD"]
INSERT_CHUNK_SIZE = 16384



# setting dates
tz = timezone('Australia/Sydney')
# now = datetime(2023, 6, 8, tzinfo=tz)
now = datetime.now(tz)
today_date = now.strftime("%Y-%m-%d")


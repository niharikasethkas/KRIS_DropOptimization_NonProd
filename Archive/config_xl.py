""" Configuration file """
import os
from pytz import timezone
import datetime
from datetime import date, datetime,timedelta

from xlwings.main import Book

# FILE PATHS #
base_path = os.path.dirname(__file__)
input_path = os.path.join(base_path, "..\\..\\input_data\\")
APP_PATH = os.path.join(base_path, "..\\")

RESULT_PATH = os.path.join(base_path, "..\\results")
RESULT_FILE = "optimal_drop_schedule.xlsm"

INPUT_PATH = os.path.join(base_path, "..\\..\\input_data\\raw_input_data\\")

TEST_PATH = os.path.join(base_path, "..\\tests")
INSTALL_PATH = os.path.join(base_path, "..\\")

os.chdir(RESULT_PATH)
wb = Book(RESULT_FILE)
sheet = wb.sheets[0]
INPUT_FILENAME = sheet.range("C9").value
EXTEND_BEYOND_PLAN_HALF = sheet.range("B9").value

ACCNT_YR_FILE = "accounting_year_info.csv"

# LOGGING #
SHARED_LOG_FILE_NAME = os.path.join(APP_PATH, "logs\\drop_frequency_optimization.log")
SHARED_LOGGING_PARAMETERS = {
    "log_file_name": SHARED_LOG_FILE_NAME,
    "file_mode": "a",
    "max_bytes": 5 * 1024 * 1024,
    "backup_count": 10,
}

# DATABASE #
RS_DB_HOSTNAME = 'redshift.kdw.int.ap-southeast-2.nonprod.a-sharedinfra.net'
RS_DB_DATABASE = 'rsskdw'
RS_DB_USER_NAME = ''
RS_DB_SCHEMA = ''

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
now = datetime.now(tz)
today_date = now.strftime("%Y-%m-%d")


# today = datetime(now.year, now.month, now.day, tzinfo=tz)
# current_day = (today - timedelta(days=today.weekday()))
# current_date = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d')
# today_date = datetime.strptime(current_date, '%Y-%m-%d')

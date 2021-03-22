from google.oauth2.service_account import Credentials
from google.cloud import storage
from datetime import datetime
from pathlib import Path
import logging
import pytz
import json
import os

DIR = os.path.realpath(os.path.dirname(__file__))
with open(f"{DIR}/../news_config.json", "r") as file:
	CONFIG = json.loads(file.read())

RAWDIR = Path(f"{DIR}/raw_data")
CLEANDIR = Path(f"{DIR}/clean_data")

CREDS = Credentials.from_service_account_file(os.environ.get(CONFIG['GCP']['ENV_CREDS_KEY']))
STORAGE_CLIENT = storage.Client(credentials=CREDS)
RAW_BUCKET = STORAGE_CLIENT.bucket("raw_items")
CLEAN_BUCKET = STORAGE_CLIENT.bucket("clean_items")

SUBSET = []

###################################################################################################

DATE = datetime.now(pytz.timezone("Canada/Eastern"))
SDATE = DATE.strftime("%Y-%m-%d")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/log.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

###################################################################################################


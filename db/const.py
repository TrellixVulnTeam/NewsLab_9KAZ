from google.cloud import storage
from datetime import datetime
from pathlib import Path
import logging
import pytz
import json
import os

DIR = os.path.realpath(os.path.dirname(__file__))
RAWDIR = Path(f"{DIR}/raw_data")

RSS_BUCKET = storage.Client().bucket("oscrap_storage")
RSS_FOLDER = Path("news_data/rss/")

BUCKET = storage.Client().bucket("cnbc-storage")
CNBC_FOLDER = Path("news_data/cnbc/")
GOOGLE_FOLDER = Path("news_data/google/")

SUBSET = []

with open(f"{DIR}/../news_config.json", "r") as file:
	CONFIG = json.loads(file.read())

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


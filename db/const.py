from google.cloud import storage
from datetime import datetime
from pathlib import Path
import logging
import pytz
import json
import os

SDIR = "/media/zquantz/0236e42e-4d80-45cf-b1f7-f12ee259facd/NewsLab"
DIR = os.path.realpath(os.path.dirname(__file__))
RAWDIR = Path(f"{SDIR}/raw_data")
UZDIR = Path(f"{SDIR}/uz_data")
ZDIR = Path(f"{SDIR}/z_data")

RSS_BUCKET = storage.Client().bucket("oscrap_storage")
RSS_FOLDER = Path(f"{SDIR}/news_data/rss/")

BUCKET = storage.Client().bucket("cnbc-storage")
CNBC_FOLDER = Path(f"{SDIR}/news_data/cnbc/")
GOOGLE_FOLDER = Path(f"{SDIR}/news_data/google/")

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


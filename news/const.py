from datetime import datetime, timedelta
from pathlib import Path
import sqlalchemy as sql
import tarfile as tar
import pandas as pd
import logging
import sys, os
import pytz
import json
import time

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
DATE = datetime.now(pytz.timezone("Canada/Eastern"))
SDATE = DATE.strftime("%Y-%m-%d")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/log.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

with open(f"{DIR}/../news_config.json", "r") as file:
	CONFIG = json.loads(file.read())

ENGINE = sql.create_engine(
    sql.engine.url.URL(
        drivername="mysql",
        username=CONFIG['SQLDB']['USER'],
        password=CONFIG['SQLDB']['PASS'],
        host=CONFIG['SQLDB']['IP'],
        port=CONFIG['SQLDB']['PORT'],
        database='compour9_finance'
    ),
    pool_size=3,
	max_overflow=0,
	pool_recycle=299,
	pool_pre_ping=True
)

###################################################################################################

from const import DIR, DATE, SDATE, ENGINE, CONFIG, logger
from datetime import datetime, timedelta
from socket import gethostname
import multiprocessing as mp
from pathlib import Path
from hashlib import md5
import pandas as pd
import numpy as np
import feedparser
import traceback
import sys, os
import pytz
import json
import uuid

import socket
socket.setdefaulttimeout(15)

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket, save_items

###################################################################################################

URL = "https://news.google.com/rss/search?q={query}+when:1h&hl=en-CA&gl=CA&ceid=CA:en"
PATH = Path(f"{DIR}/news_data")
IDSDIR = Path(f"{DIR}/ids")
FMT = "%Y-%m-%d"

news_sources = list(pd.read_csv(f"{DIR}/data/news_sources.csv").news_source)
buzzwords = pd.read_csv(f"{DIR}/data/buzzwords.csv").fillna('')

###################################################################################################

def get_id_cache():

    TDAY = datetime(DATE.year, DATE.month, DATE.day)
    file = Path(f"{DIR}/data/id_cache.json")
    
    if file.exists():

        with open(file, "r") as _file:
            id_cache = json.loads(_file.read())

        dates = list(id_cache.keys())
        for date in dates:

            dt = datetime.strptime(date, FMT)

            if (TDAY - dt).days >= 7:
            
                del id_cache[date]
                id_cache[SDATE] = []

    else:

        logger.warning(f"id cache does not exist")
        id_cache = {
            (TDAY - timedelta(days=i)).strftime(FMT) : []
            for i in range(7)
        }

    ids = set([
        _id
        for id_list in id_cache.values()
        for _id in id_list
    ])

    return id_cache, ids

def fetch(query, id_cache, ids):

	url = URL.format(query = query.replace(' ', '+'))
	try:
		feed_entries = feedparser.parse(url)
	except Exception as e:
		logger.warning(f"collection error on {query}.")
		return

	items = []
	for item in feed_entries['entries']:

		article_source = item.get('source', {})
		article_source = article_source.get('title')

		if not article_source:
			continue

		if article_source not in news_sources:
			continue

		_id = item['id']
		if _id in ids:
			continue

		ids.add(_id)
		id_cache[SDATE].append(_id)

		item['acquisition_datetime'] = datetime.utcnow().isoformat()[:19]
		item['search_query'] = query
		item['_source'] = "google"
		item['_id'] = _id
		
		items.append(item)

	if len(items) == 0:
		return

	fname = str(uuid.uuid4())
	with open(PATH / f"{fname}.json", "w") as file:
		file.write(json.dumps(items))

def collect_news(job_id, company_names, id_cache, ids, errors):

	try:

		N = len(company_names)
		for i, data in enumerate(company_names.values):

			queries = ' '.join(data)
			progress = round(i / N * 100, 2)
			logger.info(f"collecting {queries}, {progress}%")

			ticker, company_name = data
			if ticker:
				fetch(ticker, id_cache, ids)
			fetch(company_name, id_cache, ids)

	except Exception as e:

		errors.put(f"{e}\n{traceback.format_exc()}")

	with open(IDSDIR / f"{job_id}.json", "w") as file:
		file.write(json.dumps(id_cache[SDATE]))

def main():

	company_names = pd.read_csv(f"{DIR}/../clean/data/company_names.csv")
	company_names = company_names[['ticker', 'name']]

	company_names = pd.concat([company_names, buzzwords])
	company_names = company_names.reset_index(drop=True)

	chunks = np.array_split(company_names, 5)
	id_cache, ids = get_id_cache()

	errors = mp.Queue()

	processes = [
		mp.Process(target=collect_news, args=(job_id, chunk, id_cache, ids, errors))
		for job_id, chunk in enumerate(chunks)
	]

	for process in processes:
		process.start()

	for process in processes:
		process.join()

	if not errors.empty():
		error = errors.get()
		raise Exception(error)

	###############################################################################################

	for file in IDSDIR.iterdir():

		if file.name == '.gitignore':
			continue

		with open(file, "r") as _file:
			id_cache[SDATE].extend(json.loads(_file.read()))

	id_cache[SDATE] = list(set(id_cache[SDATE]))
	n_items = len(id_cache[SDATE])
	n_unique = n_items

	ids = set([
		_id
		for idlist in id_cache.values()
		for _id in idlist
	])

	with open(f"{DIR}/data/id_cache.json", "w") as file:
		file.write(json.dumps(id_cache))

	###############################################################################################

	backups = os.listdir(f"{DIR}/news_data_backup")
	xz_file = Path(f"{DIR}/news_data_backup/{SDATE}.tar.xz")
	
	if datetime.now().hour >= 10 and not xz_file.exists():

		logger.info("news job, daily save")
		n_items, n_unique = save_items(PATH, SDATE)

		if gethostname() != CONFIG['MACHINE']['HOSTNAME']:
			CONFIG['GCP']['RAW_BUCKET'] = "tmp_items"
			CONFIG['GCP']['RAW_VAULT'] = "tmp_items_vault"

		send_to_bucket(
			CONFIG['GCP']['RAW_BUCKET'],
			'news',
			xz_file,
			logger=logger
		)

		send_to_bucket(
			CONFIG['GCP']['RAW_VAULT'],
			'news',
			xz_file,
			logger=logger
		)

	logger.info("sending metrics")
	send_metric(CONFIG, "news_count", "int64_value", n_items)
	send_metric(CONFIG, "unique_news_count", "int64_value", n_unique)

if __name__ == '__main__':

	logger.info("news job, initializing")

	try:

		main()
		send_metric(CONFIG, "news_success_indicator", "int64_value", 1)

	except Exception as e:

		exc = traceback.format_exc()
		logger.warning(f"news job error, {e}, {exc}")
		send_metric(CONFIG, "news_success_indicator", "int64_value", 0)

	logger.info("news job, terminating")
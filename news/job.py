from const import DIR, DATE, SDATE, ENGINE, CONFIG, logger
from datetime import datetime, timedelta
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

URL = "https://news.google.com/rss/search?q={query}+when:1d&hl=en-CA&gl=CA&ceid=CA:en"
PATH = Path(f"{DIR}/news_data")
HASHDIR = Path(f"{DIR}/hashs")
FMT = "%Y-%m-%d"

news_sources = list(pd.read_csv(f"{DIR}/data/news_sources.csv").news_source)

###################################################################################################

def get_hash_cache():

    TDAY = datetime(DATE.year, DATE.month, DATE.day)
    file = Path(f"{DIR}/data/hash_cache.json")
    
    if file.exists():

        with open(file, "r") as _file:
            hash_cache = json.loads(_file.read())

        for date in hash_cache.keys():

            dt = datetime.strptime(date, FMT)

            if (TDAY - dt).days >= 7:
            
                del hash_cache[date]
                hash_cache[SDATE] = []

    else:

        logger.warning(f"hash cache does not exist")
        hash_cache = {
            (TDAY - timedelta(days=i)).strftime(FMT) : []
            for i in range(7)
        }

    hashs = set([
        _hash
        for hash_list in hash_cache.values()
        for _hash in hash_list
    ])

    return hash_cache, hashs

def fetch(query, hash_cache, hashs):

	url = URL.format(query = query.replace(' ', '+'))
	feed_entries = feedparser.parse(url)

	items = []
	for item in feed_entries['entries']:

		article_source = item.get('source', {})
		article_source = article_source.get('title')

		if not article_source:
			continue

		if article_source not in news_sources:
			continue

		_hash = md5(json.dumps(item).encode()).hexdigest()
		if _hash in hashs:
			continue

		hashs.add(_hash)
		hash_cache[SDATE].append(_hash)

		item['acquisition_datetime'] = datetime.now().isoformat()[:19]
		item['search_query'] = query
		item['_source'] = "google"
		
		items.append(item)

	if len(items) == 0:
		return

	fname = str(uuid.uuid4())
	with open(PATH / f"{fname}.json", "w") as file:
		file.write(json.dumps(items))

def collect_news(job_id, company_names, hash_cache, hashs, errors):

	try:

		N = len(company_names)
		for i, data in enumerate(company_names.values):

			queries = ' '.join(data)
			progress = round(i / N * 100, 2)
			logger.info(f"collecting {queries}, {progress}%")

			ticker, company_name = data
			fetch(ticker, hash_cache, hashs)
			fetch(company_name, hash_cache, hashs)

	except Exception as e:

		errors.put(f"{e}\n{traceback.format_exc()}")

	with open(HASHDIR / f"{job_id}.json", "w") as file:
		file.write(json.dumps(hash_cache[SDATE]))

def main():

	company_names = pd.read_csv(f"{DIR}/../clean/data/company_names.csv")
	company_names = company_names[['ticker', 'name']]

	chunks = np.array_split(company_names, 5)
	hash_cache, hashs = get_hash_cache()

	errors = mp.Queue()

	processes = [
		mp.Process(target=collect_news, args=(job_id, chunk, hash_cache, hashs, errors))
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

	for file in HASHDIR.iterdir():

		if file.name == '.gitignore':
			continue

		with open(file, "r") as _file:
			hash_cache[SDATE].extend(json.loads(_file.read()))

	hash_cache[SDATE] = list(set(hash_cache[SDATE]))
	n_items = len(hash_cache[SDATE])
	n_unique = n_items

	hashs = set([
		_hash
		for hashlist in hash_cache.values()
		for _hash in hashlist
	])

	with open(f"{DIR}/data/hash_cache.json", "w") as file:
		file.write(json.dumps(hash_cache))

	###############################################################################################

	now = datetime.now(pytz.timezone("Canada/Eastern"))
	backups = os.listdir(f"{DIR}/news_data_backup")
	xz_file = Path(f"{DIR}/news_data_backup/{SDATE}.tar.xz")
	
	if now.hour >= 20 and not xz_file.exists():

		logger.info("news job, daily save")
		n_items, n_unique = save_items(PATH, hashs, SDATE)

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
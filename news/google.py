from helpers import get_ticker_coordinates, get_hash_cache, save
from const import DIR, SDATE, CONFIG, logger
from datetime import datetime
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
import time
import uuid

import socket
socket.setdefaulttimeout(15)

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket

###################################################################################################

URL = "https://news.google.com/rss/search?q={query}+when:1d&hl=en-CA&gl=CA&ceid=CA:en"
news_sources = list(pd.read_csv(f"{DIR}/data/news_sources.csv").news_source)
PATH = Path(f"{DIR}/news_data/google/")
HASHDIR = Path(f"{DIR}/hashs")

###################################################################################################

def clean_item(query, item):

	cleaned_item = {
		'search_query' : query,
		'_source' : 'google'
	}

	title = item.get('title')
	if title:
		cleaned_item['title'] = title

	link = item.get('link')
	if link:
		cleaned_item['link'] = link

	published = item.get('published')
	if published:
		cleaned_item['published'] = published

	published_parsed = item.get('published_parsed')
	if published_parsed:
		iso = time.strftime('%Y-%m-%dT%H:%M:%S', tuple(published_parsed))
		cleaned_item['published_parsed'] = iso

	article_source = item.get('source', {})
	article_source = article_source.get('title')
	if article_source:
		cleaned_item['article_source'] = article_source

	source_href = item.get("source", {})
	source_href = source_href.get("href")
	if source_href:
		cleaned_item['source_href'] = source_href

	return cleaned_item

def fetch(query, hash_cache, hashs):

	url = URL.format(query = query.replace(' ', '+'))
	items = feedparser.parse(url)

	cleaned_items = []
	for item in items['entries']:

		cleaned_item = clean_item(query, item)

		if cleaned_item['article_source'] not in news_sources:
			continue

		_hash = md5(json.dumps(cleaned_item).encode()).hexdigest()
		if _hash in hashs:
			continue

		hashs.add(_hash)
		hash_cache[SDATE].append(_hash)

		cleaned_item['acquisition_datetime'] = datetime.now().isoformat()[:19]
		cleaned_items.append(cleaned_item)

	if len(cleaned_items) == 0:
		return

	fname = str(uuid.uuid4())
	with open(PATH / f"{fname}.json", "w") as file:
		file.write(json.dumps(cleaned_items))

def collect_news(job_id, ticker_coordinates, hash_cache, hashs, errors):

	try:

		N = len(ticker_coordinates)
		for i, data in enumerate(ticker_coordinates.values):

			queries = ' '.join(data)
			progress = round(i / N * 100, 2)
			logger.info(f"collecting google news, {queries}, {progress}%")

			ticker, company_name = data
			fetch(ticker, hash_cache, hashs)
			fetch(company_name, hash_cache, hashs)

	except Exception as e:

		errors.put(e)

	with open(HASHDIR / f"{job_id}.json", "w") as file:
		file.write(json.dumps(hash_cache[SDATE]))

def main():

	ticker_coordinates = get_ticker_coordinates()
	chunks = np.array_split(ticker_coordinates, 5)
	hash_cache, hashs = get_hash_cache('google')

	errors = mp.Queue()

	processes = [
		mp.Process(target=collect_news, args=(job_id, chunk[:100], hash_cache, hashs, errors))
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

	###############################################################################################

	now = datetime.now(pytz.timezone("Canada/Eastern"))
	backups = os.listdir(f"{DIR}/news_data_backup/google")

	if now.hour >= 20 and f"{SDATE}.tar.xz" not in backups:

		logger.info("google job, daily save")
		save('google', PATH, hash_cache, hashs, send_to_bucket, send_metric)

	else:

		logger.info("google job, sending metrics")
		news_data = os.listdir(f"{DIR}/news_data")
		send_metric(CONFIG, f"google_raw_news_count", "int64_value", len(news_data))
		with open(f"{DIR}/data/google_hash_cache.json", "w") as file:
			file.write(json.dumps(hash_cache))

if __name__ == '__main__':

	logger.info("google job, initializing")

	try:

		main()
		send_metric(CONFIG, "google_success_indicator", "int64_value", 1)

	except Exception as e:

		exc = traceback.format_exc()
		logger.warning(f"google job error, {e}, {exc}")
		send_metric(CONFIG, "google_success_indicator", "int64_value", 0)

	logger.info("google job, terminating")
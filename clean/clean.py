from elasticsearch.helpers.errors import BulkIndexError
from elasticsearch import Elasticsearch, helpers
from const import DIR, CONFIG, logger
from clean_item import clean_item
from importlib import reload
from hashlib import sha256
from pathlib import Path
import requests
import shutil
import sys, os
import json
import time
import uuid

sys.path.append(f"{DIR}/..")
from utils import send_metric

###################################################################################################

ES_CLIENT = Elasticsearch("localhost", port=CONFIG['ES']['PORT'], http_comprress=True, timeout=120)
HEADERS = {"Content-Type" : "application/json"}

NEWS_DIRS = [
	Path(f"{DIR}/../rss/news_data"),
	Path(f"{DIR}/../news/news_data"),
]

NEWS_DIR = Path(f"{DIR}/news_data")
CLEAN_DIR = Path(f"{DIR}/clean_data")

###################################################################################################

def get_files(files):
	
	return [
		shutil.copy(file, NEWS_DIR / file.name)
		for _dir in NEWS_DIRS
		for file in list(_dir.iterdir())
		if
		(
			(NEWS_DIR / file.name) not in files
			and
			file.name != '.gitignore'
		)
	]

def get_scores(sentences):

	data = {"sentences" : sentences}
	response = requests.post("http://localhost:9602", headers=HEADERS, json=data)
	response = json.loads(response.content)
	return response.values()

def cleaning_loop():

	ctr = 0
	files = {NEWS_DIR / ".gitignore"}
	n_clean = len(list(CLEAN_DIR.iterdir()))

	while True:

		new_files = get_files(files)
		n_clean_new = len(list(CLEAN_DIR.iterdir()))

		if n_clean_new < n_clean:
			files = {NEWS_DIR / ".gitignore"}
			reload(sys.modules['clean_item'])
			reload(sys.modules['find_company_names'])
			logger.info("reloading the company names")
				
		items = []
		for new_file in new_files:
			with open(new_file, "r") as file:
				try:
					items.extend(json.loads(file.read()))
					files.add(new_file)
				except Exception as e:
					logger.warning(f"File read error. {e}")

		new_items = []
		for item in items:

			if not item.get("title"):
				continue

			_id = item['_id']
			if item['_source'] == 'google':
				_id = sha256(_id.encode()).hexdigest()

			item = clean_item(item)
			new_items.append({
				"_index" : "news",
				"_id" : _id,
				"_op_type" : "create",
				"_source" : item
			})

		if len(new_items) != 0:

			titles = [
				item['_source']['title']
				for item in new_items
			]
			scores = get_scores(titles)

			for item, score in zip(new_items, scores):
				item['_source']['sentiment'] = score['prediction']
				item['_source']['sentiment_score'] = score['sentiment_score']
				item['_source']['abs_sentiment_score'] = abs(score['sentiment_score'])

			successes, failures = helpers.bulk(ES_CLIENT,
											   new_items,
											   stats_only=True,
											   raise_on_error=False)
			
			print(successes, failures)
			with open(CLEAN_DIR / f"{str(uuid.uuid4())}.json", "w") as file:
				file.write(json.dumps(new_items))

			new_items = []

		###########################################################################################

		if ctr % 10 == 0:

			try:
				
				send_metric(
					CONFIG,
					"rss_counter",
					"int64_value",
					len(list(NEWS_DIRS[0].iterdir())) - 1
				)
				ctr = 0

			except Exception as e:

				logger.warning(e)

		###########################################################################################

		ctr += 1
		time.sleep(5)
		n_clean = n_clean_new

if __name__ == '__main__':

	cleaning_loop()

from const import logger, STORAGE_CLIENT, DIR, CONFIG
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from pathlib import Path
import tarfile as tar
import pandas as pd
import sys, os

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket

es = Elasticsearch(port=8607, timeout=60_000)

def get_data(date):

	start = f"{(date - timedelta(days=2)).isoformat()[:10]}T21:00:00"
	end = f"{(date - timedelta(days=1)).isoformat()[:10]}T20:59:59"

	query = {
		"query" : {
			"bool" : {
				"must" : [
					{
						"exists" : {
							"field" : "tickers"
						}
					}
				],
				"filter" : {
					"range" : {
						"published_datetime" : {
							"gte" : start,
							"lte" : end
						}
					}
				} 
			},
		},
		"size" : 1_000,
		"_source" : ["tickers", "sentiment_score"]
		
	}

	response = es.search(query, index="news", scroll="3m")
	items = response['hits']['hits']

	while len(response['hits']['hits']) > 0:

		response = es.scroll(scroll_id = response.get("_scroll_id"), scroll="3m")
		items.extend(response['hits']['hits'])

	return [item['_source'] for item in items]

def process_data(data):

	df = pd.DataFrame(data)
	df = df.explode('tickers').reset_index()
	df = df.rename({'index' : 'item'}, axis=1)
	pre_n = len(df)

	x = df.tickers.str.split(":", expand=True)
	df['tickers'] = x[1].combine_first(x[0])
	df['tickers'] = df.tickers.str.strip()
	df = df[df.tickers.str[0].str.isalpha()]
	df = df.drop_duplicates(subset=['item', 'tickers'])

	x = df.item.value_counts()
	df = df[df.item.isin(x[x<=3].index)]

	df['ntickers'] = df.item.map(x.to_dict())
	df['sentiment_score'] = df.sentiment_score / df.ntickers.map({
		1 : 1,
		2 : 0.75,
		3 : 0.5
	})

	df = df.groupby('tickers').agg({
		'sentiment_score' : ['count', 'mean']
	}).reset_index()
	df.columns = ['ticker', 'volume', 'sentiment']
	df['sentiment'] = df.sentiment.round(4)

	return pre_n, df.sort_values('volume', ascending=False)

def main(date):

	logger.info("News Stats Initiated")
	datestr = (date - timedelta(days=1)).isoformat()[:10]
	file = Path(f"{DIR}/data/{datestr}.csv")
	xz_file = file.with_suffix(".tar.xz")

	try:

		logger.info(f"Processing stats for {date}")
		
		pre_n, df = process_data(get_data(date))
		df['date'] = datestr
		df = df[['date', 'ticker', 'volume', 'sentiment']]

		logger.info(f"Processed stats for {len(df)} tickers. Collected {pre_n} items.")
		send_metric(CONFIG, "news_stats_ticker_pre_count", "int64_value", pre_n)
		send_metric(CONFIG, "news_stats_ticker_post_count", "int64_value", len(df))

		if len(df) == 0:
			raise Exception("Zero tickers after filtering")

		df.to_csv(file, index=False)
		with tar.open(xz_file, "x:xz") as tar_file:
			tar_file.add(file, arcname=file.name)

		send_to_bucket(
			"daily_news_stats",
			"", 
			xz_file, 
			logger=logger
		)

		os.unlink(file)
		os.unlink(xz_file)

		send_metric(CONFIG, "news_stats_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.info(f"News Stats Error - {e}")
		send_metric(CONFIG, "news_stats_success_indicator", "int64_value", 0)


	logger.info("News Stats Terminated")

def once():

	s = datetime(2020, 4, 1)
	now = datetime(2021, 9, 2)
	while s < now:
		main(s)
		s = s + timedelta(days=1)
		print(s.isoformat()[:10])

if __name__ == '__main__':

	main(datetime.now())

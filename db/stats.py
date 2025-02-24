from const import logger, STORAGE_CLIENT, DIR, CONFIG
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from pathlib import Path
import tarfile as tar
import pandas as pd
import sys, os

sys.path.append(f"{DIR}/..")
from utils import send_metric, send_to_bucket

ES_CLIENT = Elasticsearch(
	"http://{USER}:{KEY}@{HOST}:{PORT}".format(**CONFIG['ES_READER']),
	http_comprress=True,
	timeout=10000
)
sector_to_ticker = {
	'Consumer Cyclical': 'SECTORY',
	'Technology': 'SECTORK',
	'Communication Services': 'SECTORC',
	'Healthcare': 'SECTORV',
	'Industrials': 'SECTORI',
	'Consumer Defensive': 'SECTORD',
	'Financial Services': 'SECTORF',
	'Energy': 'SECTORE',
	'Utilities': 'SECTORU',
	'Real Estate': 'SECTORRE',
	'Basic Materials': 'SECTORB',
	'Industrial Goods': 'SECTORIG',
	'Services': 'SECTORS',
}

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

	response = ES_CLIENT.search(query, index="news", scroll="3m")
	items = response['hits']['hits']

	while len(response['hits']['hits']) > 0:

		response = ES_CLIENT.scroll(scroll_id = response.get("_scroll_id"), scroll="3m")
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
	df = df[df.tickers.str[0].str.isalpha().astype(bool)]
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

	return pre_n, df

def get_ticker_info():

	for blob in STORAGE_CLIENT.bucket("daily_ticker_info2").list_blobs():
		pass

	csv_file = Path(f"{DIR}/{blob.name.split('.')[0]}.csv")
	xz_file = csv_file.with_suffix(".tar.xz")

	blob.download_to_filename(xz_file)
	with tar.open(xz_file, "r:xz") as tar_file:

import os

def is_within_directory(directory, target):
	
	abs_directory = os.path.abspath(directory)
	abs_target = os.path.abspath(target)

	prefix = os.path.commonprefix([abs_directory, abs_target])
	
	return prefix == abs_directory

def safe_extract(tar, path=".", members=None, *, numeric_owner=False):

	for member in tar.getmembers():
		member_path = os.path.join(path, member.name)
		if not is_within_directory(path, member_path):
			raise Exception("Attempted Path Traversal in Tar File")

	tar.extractall(path, members, numeric_owner=numeric_owner) 
	

safe_extract(tar_file, path=DIR)

	df = pd.read_csv(csv_file)

	os.unlink(csv_file.__str__())
	os.unlink(xz_file.__str__())
	return df

def create_sector_tickers(volume, info):

	usectors = info.sector.dropna().unique()
	sectors = info[['ticker', 'sector']].merge(volume, how='left', on='ticker').dropna()
	sectors = sectors.groupby('sector').agg({'sentiment': 'mean', 'volume': 'sum'}).reset_index()
	sectors = pd.concat([
		sectors,
		pd.DataFrame([
			[sector, 0, 0]
			for sector in usectors
			if sector not in sectors.sector.unique()
		], columns = ['sector', 'sentiment', 'volume'])
	]).sort_values('volume', ascending=False).reset_index(drop=True)
	sectors['ticker'] = sectors.sector.map(sector_to_ticker)
	return pd.concat([
		volume,
		sectors[['ticker', 'sentiment', 'volume']]
	]).sort_values('volume', ascending=False).reset_index(drop=True)

def main(date):

	logger.info("News Stats Initiated")
	datestr = (date - timedelta(days=1)).isoformat()[:10]
	file = Path(f"{DIR}/data/{datestr}.csv")
	xz_file = file.with_suffix(".tar.xz")

	try:

		logger.info(f"Processing stats for {date}")
		
		pre_n, df = process_data(get_data(date))
		df = create_sector_tickers(df, get_ticker_info())
		pre_n += len(sector_to_ticker)

		df['date'] = datestr
		df['volume'] = df.volume.astype(int)
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

		os.unlink(file.__str__())
		os.unlink(xz_file.__str__())

		send_metric(CONFIG, "news_stats_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.info(f"News Stats Error - {e}")
		send_metric(CONFIG, "news_stats_success_indicator", "int64_value", 0)


	logger.info("News Stats Terminated")

def once():

	s = datetime(2020, 4, 1)
	now = datetime(2022, 5, 23)
	while s < now:
		main(s)
		s = s + timedelta(days=1)
		print(s.isoformat()[:10])

if __name__ == '__main__':

	main(datetime.now())

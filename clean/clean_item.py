from datetime import datetime, timedelta, timezone
from find_company_names import find_company_names
from urllib.parse import urlparse
from const import DIR, logger
from bs4 import BeautifulSoup
from langid import classify
from hashlib import sha256
from pathlib import Path
from pprint import pprint
import pandas as pd
import dateparser
import sys, os
import time
import uuid
import re

###################################################################################################

df = pd.read_csv(f"{DIR}/data/company_names.csv")
df['fullcode'] = df.exchange + ":" + df.ticker

fullcode_set = set(df.fullcode)
ticker_set = set(df.ticker)
href_set = {"stock", "stocks", "symbol"}

df = pd.read_csv(f"{DIR}/data/exchanges.csv")
exchange_set = df.Acronym.dropna().tolist()
exchange_set += df['Exchange Name'].dropna().tolist()

extra_exchange_set = ["Oslo", "Paris", "Helsinki", "Copenhagen", "OTC", "OTCQX"]
extra_exchange_set += ["OTCQB", "Stockholm", "CNSX", "OTC Markets", "Brussels"]
extra_exchange_set += ["Frankfurt", "Amsterdam", "Iceland", "Vilnius", "Tallinn"]
extra_exchange_set += ["Luxembourg", "Irish", "Riga", "Symbol"]
extra_exchange_set = [exch.upper() for exch in extra_exchange_set]

exchange_set += extra_exchange_set
exchange_set = [re.sub("-|\.", "", exch) for exch in exchange_set]

TICKER_PAT = "[A-Z\.-]{3,15}[\s]{0,1}:[\s]{0,1}[A-Z\.-]{1,15}"
TICKER_PAT2 = "\((?:Symbol|Nasdaq|Euronext)[\s]{0,1}:[\s]{0,1}[A-Z\.-]+\)"
SUB_PAT = "<pre(.*?)</pre>|<img(.*?)/>|<img(.*?)>(.*?)</img>|</br>"

DEFAULT_TIME = "1970-01-01T00:00:00"
DATE_FMTS = [
	"%a, %d %b %Y %H:%M %Z",
	"%a, %d %b %Y %H:%M %z",
	"%Y-%m-%d %H:%M:%S",
	"%Y-%d-%m %H:%M:%S",
	"%Y-%m-%dT%H:%M:%S",
]

###################################################################################################

def validate(match, hit, miss):

	if match.count(":") == 1:
		match = re.sub(" : |: | :", ":", match)
		exch, ticker = match.split(":")
		exch = re.sub("-|\.|Other ", "", exch).upper()
		match = f"{exch}:{ticker}"

	if match in fullcode_set:
		hit.append(match)
	elif ":" in match and match.split(":")[0] in exchange_set:
		hit.append(match)
	elif match in ticker_set:
		hit.append(match)
	else:
		miss.append(match)

	return match

def clean_item(item):

	ticker_matches = []
	ticker_misses = []
	categories = []
	_authors = []
	contribs = []
	tables = []

	source = item['_source']
	is_og_rss = source == "rss" and item['feed_source'] != 'Google'

	###############################################################################################
	## Link Cleaning

	links = item.get('links')
	if links:
		item['link'] = links[0]

	link = item.get('link')
	if link:
		if type(link) is dict:
			item['link'] = item['link']['href']
		item['link'] = item['link'].lower()

	###############################################################################################
	## RSS Specific

	if source == "rss":

		article_source = urlparse(item['link']).netloc
		item['article_source'] = article_source.split(".")[1]

	if is_og_rss:

		_authors.append(item.get("author"))
		for author in item.get("authors", []):
			_authors.append(author.get('name'))

		_authors.append(item.get("author_detail", {"name" : None}).get('name'))
		_authors.append(item.get("publisher"))

		_authors = [author for author in _authors if author]

		for contributor in item.get("contributors", []):
			contribs.append(contributor.get('name'))

		keyword = item.get('dc_keyword')
		if keyword:	
			categories.append(keyword)

		###############################################################################################
		## Tickers & Categories from tags

		for tag in item.get('tags', []):

			if not tag['scheme']:
				continue

			if "ISIN" in tag['scheme']:
				continue

			if "http" in tag['scheme']:

				url = tag['scheme'].split("/")[3:]
				url = set(url)

				if len(url.intersection(href_set)) == 1:
					validate(tag['term'], ticker_matches, ticker_misses)

				elif "taxonomy" in url:

					finds = re.findall("\s([A-Z]+)\s", f" {tag['term']} ")
					if len(finds) == 1:						
						validate(tag['term'], ticker_matches, ticker_misses)

			elif tag['scheme'] == "stock-symbol":

				validate(tag['term'], ticker_matches, ticker_misses)

			else:

				categories.append(tag['term'])

		###############################################################################################
		## NASDAQ Tickers

		tickers = item.get('nasdaq_tickers')
		if tickers:

			tickers = tickers.split(",")
			for ticker in tickers:
				
				if ":" not in ticker:
					ticker = "NASDAQ:" + ticker

				validate(ticker, ticker_matches, ticker_misses)

		###############################################################################################
		## HTML Summary

		summary = item.get('summary', '')
		_summary = BeautifulSoup(summary, "lxml")

		a_tags = _summary.find_all("a")
		for a_tag in a_tags:

			text = f" {a_tag.text} "
			classes = a_tag.get("class", [""])
			href = set(a_tag.get("href", "").split("/")[3:])
			finds = re.findall("\s([A-Z]+)\s", text)

			if len(finds) != 1 or ' ' in a_tag.text:
				continue

			text = text.strip()
			if 'ticker' in classes or len(href.intersection(href_set)) >= 1:
				text = validate(text, ticker_matches, ticker_misses)

			a_tag.replace_with(_summary.new_string(text))

		summary = str(_summary)

		fullcodes = re.findall(TICKER_PAT, summary)
		for fullcode in fullcodes:
			
			text = validate(fullcode, ticker_matches, ticker_misses)
			summary = summary.replace(fullcode, text)

		symbols = re.findall(TICKER_PAT2, summary)
		for symbol in symbols:
			text = validate(symbol[1:-1], ticker_matches, ticker_misses)

		###############################################################################################
		## Summary Part 2

		summary = re.sub(SUB_PAT, "", str(summary))
		_summary = BeautifulSoup(summary, "lxml")

		_tables = _summary.find_all("table")
		for table in _tables:
			tables.append(str(table))
			table.replace_with(_summary.new_string(""))

		xls = _summary.find_all("ul")
		xls += _summary.find_all("ol")
		for xl in xls:
			
			xl_str = ""
			lis = xl.find_all("li")
			
			for li in lis:

				li = li.text.strip()
				
				if len(li) == 0:
					continue

				if li[-1] not in ";.,:?!":
					li += "."

				xl_str += f"{li} "

			xl.replace_with(_summary.new_string(xl_str.strip()))

		summary = ""
		ctr = 0
		for string in _summary.strings:

			summary = summary.strip()
			if string == '\n':
				ctr += 1
			else:
				ctr = 0

			if len(summary) > 0 and ctr > 2 and summary[-1] not in ".:;?!":
				summary = summary + f". {string}"
			else:
				summary = summary + f" {string}"

	###############################################################################################
	## Tickers

	tickers = re.findall(TICKER_PAT, item['title'])
	tickers.extend(find_company_names(item['title']))
	if is_og_rss:
		tickers.extend(find_company_names(summary))

	tickers.extend([
		ticker[1:-1]
		for ticker in re.findall(TICKER_PAT2, item['title'])
	])

	for ticker in tickers:
		validate(ticker, ticker_matches, ticker_misses)

	###############################################################################################
	## Time Stuff

	timestamp = item.get('published', item.get('updated', DEFAULT_TIME))
	try:
		timestamp = dateparser.parse(timestamp, DATE_FMTS)
		tz = (
			timedelta(seconds=0) 
			if not timestamp.utcoffset()
			else timestamp.utcoffset()
		)
		timestamp += tz
		timestamp = timestamp.replace(tzinfo=None)
	except Exception as e:
		timestamp = datetime.strptime(DEFAULT_TIME, DATE_FMTS[-1])
		logger.warning(f"time conversion error,{e}")

	oscrap_timestamp = item.get('acquisition_datetime', DEFAULT_TIME)
	oscrap_timestamp = datetime.strptime(oscrap_timestamp[:19], DATE_FMTS[-1])

	oscrap_timestamp = oscrap_timestamp.isoformat()[:19]
	timestamp = timestamp.isoformat()[:19]

	###############################################################################################
	## Language

	language = item.get('language', classify(item['title'])[0])

	###############################################################################################
	## Create new object

	new_item = {
		'title' : item['title'].strip(),
		'timestamp' : timestamp,
		'oscrap_timestamp' : oscrap_timestamp,
		'language' : language,
		'link' : item['link'].lower(),
		'article_source' : item['article_source'].lower(),
		'source' : source
	}

	if is_og_rss and summary:
		new_item['summary'] = summary.strip()
		new_item['_summary'] = item['summary'].strip()

	if ticker_matches:
		for ticker in ticker_matches:
			if ':' in ticker:
				ticker_matches.append(ticker.split(':')[1])
		new_item['tickers'] = list(set(ticker_matches))

	if ticker_misses:
		new_item['_tickers'] = list(set(ticker_misses))

	if categories:
		new_item['categories'] = [
			cat.lower()
			for cat in list(set(categories))
		]

	if _authors:
		new_item['authors'] = [
			author.lower()
			for author in
			list(set(_authors)) + [item['article_source']]
		]

	if contribs:
		new_item['related'] = list(set(contribs))

	if tables:
		new_item['tables'] = tables

	if item.get('credit'):
		new_item['credit'] = item['credit']

	###############################################################################################
	## Search field

	search = [new_item['title']]

	summary = new_item.get("summary")
	if summary:
		search.append(summary)

	cats = new_item.get("categories")
	if cats:
		search.extend(cats)

	tickers = new_item.get('tickers')
	if tickers:
		search.extend(tickers)

	new_item['search'] = search

	print(new_item)

	return new_item

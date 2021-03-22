from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET, RAWDIR, UZDIR, ZDIR
from filetype import guess
from pathlib import Path
from hashlib import md5
import tarfile as tar
import pandas as pd
import sys, os
import json

COLS = [
	'Unnamed: 0',
	'title',
	'links',
	'link',
	'id',
	'guidislink',
	'published',
	'published_parsed',
	'summary',
	'title_detail.type',
	'title_detail.language',
	'title_detail.base',
	'title_detail.value',
	'summary_detail.type',
	'summary_detail.language',
	'summary_detail.base',
	'summary_detail.value',
	'source.href',
	'source.title'
]

news_sources = set(pd.read_csv("../news/data/news_sources.csv").news_source)

def init_dirs(DIR):

	if not DIR.is_dir():
		DIR.mkdir()

	_dir = (DIR / "google")
	if not _dir.is_dir():
		_dir.mkdir()

def download():

	for blob in BUCKET.list_blobs():

		if 'twitter' in blob.name.lower():
			continue

		parent, name = blob.name.split("/")
		if not name:
			continue

		if parent == "GoogleNews":

			file = RAWDIR / "google" / name
			if not file.exists():

				print("Downloading Google:", name)
				blob.download_to_filename(file)

				ftype = guess(str(file))
				if ftype:
					print(ftype)
					with tar.open(file, "r:gz") as tar_file:
						tar_file.extractall(UZDIR / "google")
				else:
					print("FAULT ON", file)

def normalize_df(data):

	items = []
	cols = data.columns

	for row in data.values:
		
		item = {}
		for col, val in zip(cols, row):
			
			if pd.isna(val):
				continue
			
			col = col.split(".")
			if len(col) == 1:
				item[col[0]] = val
			else:
				parent, child = col
				d = {child : val}
				item[parent] = {**item.get(parent, {}), **d}
			
		if 'links' in item:
			item['links'] = eval(item['links'])
		
		items.append(item)

	return items

def normalize_dfs():

	for file in sorted((UZDIR / "google").iterdir()):

		if 'json' in file.name:
			continue

		try:
			data = pd.read_csv(file, nrows=2)
		except Exception as e:
			print(e)
			continue

		if 'title' not in data.columns:
			data = pd.read_csv(file, names=COLS)
		else:
			data = pd.read_csv(file)

		print(data[data.title == 'title'].shape)
		data = data.drop(["Unnamed: 0"], axis=1)
		data = data[data.title != 'title']

		print(file)
		items = normalize_df(data)

		with open((UZDIR / "google" / file.name).with_suffix(".json"), "w") as _file:
			_file.write(json.dumps(items))

def merge_files():

	stats = []
	for file in (UZDIR / "google").iterdir():

		if '.csv' in file.name:
			continue

		n = file.name.count("_")
		date = file.name.split("_")[n].split(" ")[0]
		stats.append([file, date])
		
	df = pd.DataFrame(stats, columns = ['file', 'date'])
	dates = df.groupby('date')['file'].apply(list).to_dict()

	for key in dates:
		
		items = []
		for file in dates[key]:
			print(key, file)
			with open(file, "r") as _file:
				items.extend(json.loads(_file.read()))

		with open(file.parent / f"{key}.json", "w") as _file:
			_file.write(json.dumps(items))

		print()
		print()

def remove_duplicates():

	ids = set()
	for file in sorted((UZDIR / "google").iterdir()):

		source = 0
		unique = 0
		if '_' in file.name:
			continue

		print(file.name)
		with open(file, "r") as _file:
			items = json.loads(_file.read())

		new_items = []
		for item in items:

			article_source = item.get('source', {})
			article_source = article_source.get('title')
			if article_source not in news_sources:
				source += 1
				continue

			_id = item['id']
			if _id in ids:
				unique += 1
				continue

			ids.add(_id)
			new_items.append(item)

		print(len(items), len(new_items), source, unique)

		with open(file, "w") as _file:
			_file.write(json.dumps(new_items))

		print()

def add_fields():

	for file in sorted((UZDIR / "google").iterdir()):

		if '_' in file.name:
			continue	

		print(file.name)
		with open(file, "r") as _file:
			items = json.loads(_file.read())

		n_items = 0
		for item in items:

			item['acquisition_datetime'] = "1970-01-01T00:00:00"
			item['_source'] = 'google'
			item['search_query'] = ''
			item['_id'] = item['id']
			n_items += 1

		print(n_items)
		print()
		with open(file, "w") as _file:
			_file.write(json.dumps(items))

def compress():

	for file in (UZDIR / "google").iterdir():

		if '_' in file.name:
			continue

		print(file.name)
		with tar.open(ZDIR / "google" / (file.with_suffix(".tar.xz").name), "x:xz") as tar_file:
			tar_file.add(file, arcname=file.name)

if __name__ == '__main__':

	# init_dirs(RAWDIR)
	# init_dirs(UZDIR)
	# init_dirs(ZDIR)

	# print("\n\n\n\n\n\nDOWNLOAD")
	# download()
	
	# print("\n\n\n\n\n\nNORMALIZE DFS")
	# normalize_dfs()
	
	# print("\n\n\n\n\n\nMERGE FILES")
	# merge_files()

	# print("\n\n\n\n\n\nRemove Duplicates")
	# remove_duplicates()
	
	# print("\n\n\n\n\n\nAdd Fields")
	# add_fields()
	
	print("\n\n\n\n\n\nCompress")
	compress()
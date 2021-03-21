from const import RSS_FOLDER, CNBC_FOLDER, GOOGLE_FOLDER
from const import RSS_BUCKET, BUCKET, RAWDIR, UZDIR, ZDIR
from datetime import datetime, timedelta
from filetype import guess
from pathlib import Path
from hashlib import md5
import tarfile as tar
import pandas as pd
import sys, os
import json

feeds = pd.read_csv("../rss/data/rss.csv")
feeds = feeds.groupby("Rss Feed")['Source'].apply(list).to_dict()

def init_dirs(DIR):

	if not DIR.is_dir():
		DIR.mkdir()

	_dir = (DIR / "rss")
	if not _dir.is_dir():
		_dir.mkdir()

def download():

	## RSS
	for blob in RSS_BUCKET.list_blobs():

		if 'rss' not in blob.name or 'cleaned' in blob.name:
			continue

		parent, name = blob.name.split("/")
		file = RAWDIR / "rss" / name

		if not file.exists():

			print("Downloading rss:", blob.name)
			blob.download_to_filename(file)

			with tar.open(file, "r:xz") as tar_file:
				tar_file.extractall(UZDIR / "rss")

def add_fields():

	KEYS = [
		'links',
		'link',
		'updated',
		'published',
		'published_parsed'
	]

	def get_id(item):

		for key in KEYS:
			if key in item:
				item.pop(key)

		_hash = json.dumps(item, sort_keys = True).encode()
		return md5(_hash).hexdigest()

	for file in sorted((UZDIR / "rss").iterdir()):

		print(file.name)

		with open(file, "r") as _file:
			items = json.loads(_file.read())

		new_items = []
		for item in items:

			item['id'] = get_id(item.copy())

			if 'oscrap_acquisition_datetime' in item:
				dt = item['oscrap_acquisition_datetime']
				dt = datetime.strptime(dt, "%Y-%d-%m %H:%M:%S.%f")
				dt -= timedelta(hours=5)
				item['acquisition_datetime'] = dt.isoformat()[:19]
				del item['oscrap_acquisition_datetime']
			else:
				item['acquisition_datetime'] = "1970-01-01T00:00:00"

			if 'title_detail' in item:
				item['feed_source'] = feeds.get(item['title_detail']['base'], [''])[0]
			else:
				item['feed_source'] = feeds.get(item['summary_detail']['base'], [''])[0]

			item['_source'] = 'rss'
			new_items.append(item)

		with open(file, "w") as _file:
			_file.write(json.dumps(new_items))

def rename():

	for file in sorted((UZDIR / "rss").iterdir()):
		file.rename(file.with_suffix(".json"))

def compress():

	for file in (UZDIR / "rss").iterdir():

		print(file.name)
		with tar.open(ZDIR / "rss" / (file.with_suffix(".tar.xz").name), "x:xz") as tar_file:
			tar_file.add(file, arcname=file.name)

if __name__ == '__main__':

	print("INIT DIRS")
	init_dirs(RAWDIR)
	init_dirs(UZDIR)
	init_dirs(ZDIR)

	print("DOWNLOAD")
	download()

	print("ADD FIELDS")
	add_fields()

	print("RENAME TO JSON")
	rename()

	# print("COMPRESS")
	# compress()

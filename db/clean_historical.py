from const import DIR, RAWDIR, RAW_BUCKET, CONFIG, CLEANDIR
from joblib import delayed, Parallel
from pathlib import Path
from hashlib import md5
import tarfile as tar
import requests
import json
import time
import sys

sys.path.append(f"{DIR}/../clean")
from clean_item import clean_item

###################################################################################################

def download():

	if not RAWDIR.exists():
		RAWDIR.mkdir()

	if not (RAWDIR / "rss").exists():
		(RAWDIR / "rss").mkdir()

	if not (RAWDIR / "news").exists():
		(RAWDIR / "news").mkdir()

	for blob in RAW_BUCKET.list_blobs():

		print(blob.name)
		parent, name = blob.name.split("/")

		if not name:
			continue
		if name != '2022-02-25.tar.xz':
			continue

		filename = RAWDIR / parent / name
		blob.download_to_filename(filename)

		with tar.open(filename, "r:xz") as tar_file:
			tar_file.extractall(RAWDIR / parent)

		filename.unlink()

###################################################################################################

def clean(job_id, files, cleaner):

	for file in sorted(files)[::-1]:

		if file.name == '.gitignore':
			continue

		print(job_id, "rss_file", file.name)
		with open(file, "r") as _file:
			items = json.loads(_file.read())

		news_file = file.parent.parent / "news" / file.name
		if news_file.exists():
			print(job_id, "news_file", news_file.name, len(items))
			with open(news_file, "r") as _file:
				items.extend(json.loads(_file.read()))
			print(job_id, len(items))

		clean_items = []
		for i, item in enumerate(items[::-1]):

			if not item.get('title'):
				continue

			clean_items.append(cleaner(item))

		cleaner_items = []
		ids = set()
		for item in clean_items:

			dummy_item = {
				'title' : item['title'],
				'article_source' : item['article_source'],
				'published_datetime' : item['published_datetime'][:10]
			}
			if 'summary' in item:
				dummy_item['summary'] = item['summary']

			_id = md5(json.dumps(dummy_item).encode()).hexdigest()

			if _id in ids:
				continue

			cleaner_items.append({
				"_index" : "news",
				"_id" : _id,
				"_op_type" : "create",
				"_source" : item
			})

		print(job_id, len(items), len(clean_items), len(cleaner_items))

		with open(CLEANDIR / "news" / file.name, "w") as _file:
			_file.write(json.dumps(cleaner_items))

def clean_items():

	if not CLEANDIR.exists():
		CLEANDIR.mkdir()

	n_jobs = 2
	files = list((RAWDIR / "rss").iterdir())
	files += list((RAWDIR / "news").iterdir())
	files = sorted(files)
	print(len(files))
	files = [
		file
		for file in files
		if not (file.parent.parent.parent / "clean_data" / file.name).exists()
	]
	print(len(files))

	chunk_size = int(len(files) / n_jobs)
	chunks = [
		files[i - chunk_size : i][::-1]
		for i in range(chunk_size, len(files) + chunk_size, chunk_size)
	]

	Parallel(n_jobs=n_jobs)(
		delayed(clean)(i, chunk.copy(), clean_item)
		for i, chunk in enumerate(chunks)
	)

###################################################################################################

def get_scores(sentences, host):

	data = {"sentences" : sentences}
	response = requests.post(f"http://{host}:9602", headers={"Content-Type" : "application/json"}, json=data)
	response = json.loads(response.content)
	return response.values()

def score_batch(job_id, files, get_scores, host):

	for i, file in enumerate(files):

		if file.name == '.gitignore:':
			continue

		print(job_id, "Processing", file.name)
		with open(file, "r") as _file:
			items = json.loads(_file.read())

		if all('_source' in item and 'sentiment' in item['_source'] for item in items):
			print(job_id, "Already Processed")
			continue

		titles = [
			item['_source']['title']
			for item in items
		]
		scores = get_scores(titles, host)

		for item, score in zip(items, scores):
			item['_source']['sentiment'] = score['prediction']
			item['_source']['sentiment_score'] = score['sentiment_score']
			item['_source']['abs_sentiment_score'] = abs(score['sentiment_score'])

		with open(file, "w") as _file:
			_file.write(json.dumps(items))

		print(job_id, "Progress:", round(i / len(files) * 100, 2))

def get_sentiment_scores():

	p = Path(f"{DIR}/cdata/news")
	files = list(p.iterdir())
	files.remove(p / ".gitignore")
	
	n = len(files)
	chunks = [
		files[:int(n/2)],
		files[int(n/2):]
	]
	chunks = [files]

	Parallel(n_jobs=1)(
		delayed(score_batch)(job_id, chunk, get_scores, host)
		for job_id, (chunk, host) in enumerate(zip(chunks, ['localhost']))
	)

def compress_files():

	p = Path(f"{DIR}/cdata/news")
	for file in p.iterdir():
		print("Processing", file.name)
		xz_file = file.with_suffix(".tar.xz")
		with tar.open(xz_file, "x:xz") as tar_file:
			tar_file.add(file, arcname=file.name)

if __name__ == '__main__':

	# download()
	# clean_items()
	# get_sentiment_scores()
	compress_files()
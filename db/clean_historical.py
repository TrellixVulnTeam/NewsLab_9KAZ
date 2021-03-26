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

		filename = RAWDIR / parent / name
		blob.download_to_filename(filename)

		with tar.open(filename, "r:xz") as tar_file:
			tar_file.extractall(RAWDIR / parent)

		filename.unlink()

def clean(job_id, files, cleaner):

	for file in sorted(files)[::-1]:

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

		with open(CLEANDIR / file.name, "w") as _file:
			_file.write(json.dumps(cleaner_items))

def clean_items():

	if not CLEANDIR.exists():
		CLEANDIR.mkdir()

	n_jobs = 12
	files = list((RAWDIR / "rss").iterdir())
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

def get_sentiment_scores():

	def get_scores(sentences):

		data = {"sentences" : sentences}
		response = requests.post("http://192.168.2.186:9602", headers={"Content-Type" : "application/json"}, json=data)
		response = json.loads(response.content)
		return response.values()

	processed = []
	p = Path(f"{DIR}/clean_data")
	while True:

		for file in p.iterdir():

			if file in processed:
				continue
			time.sleep(15)
			print("Processing", file.name)
			with open(file, "r") as _file:
				items = json.loads(_file.read())

			titles = [
				item['title']
				for item in items
			]
			scores = get_scores(titles)

			for item, score in zip(items, scores):
				item['sentiment'] = score['prediction']
				item['sentiment_score'] = score['sentiment_score']
				item['abs_sentiment_score'] = abs(score['sentiment_score'])

			with open(file, "w") as _file:
				_file.write(json.dumps(items))

			processed.append(file)
			print(len(processed))
			print()
			time.sleep(10)

if __name__ == '__main__':

	# download()
	clean_items()
	# get_sentiment_scores()
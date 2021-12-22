from const import CONFIG, SUBSET, CLEANDIR, CLEAN_BUCKET
from elasticsearch import Elasticsearch, helpers
import tarfile as tar
import json

###################################################################################################

ES_MAPPINGS = {
	"settings": {
		"number_of_shards": 1,
		"number_of_replicas": 0,
		"analysis" : {
			"analyzer" : {
				"index_analyzer" : {
					"tokenizer" : "classic",
					"filter" : [	
						"classic",
						"lowercase",
						"stop",
						"trim",
						"porter_stem",
						"index_shingler",
						"unique"
					]
				},
				"search_analyzer" : {
					"tokenizer" : "classic",
					"filter" : [	
						"classic",
						"lowercase",
						"stop",
						"trim",
						"porter_stem",
						"search_shingler",
						"unique"
					]
				}

			},
			"filter" : {
				"index_shingler" : {
					"type" : "shingle",
					"min_shingle_size" : 2,
					"max_shingle_size" : 3,
					"output_unigrams" : True
				},
				"search_shingler" : {
					"type" : "shingle",
					"min_shingle_size" : 2,
					"max_shingle_size" : 3,
					"output_unigrams_if_no_shingles" : True,
					"output_unigrams" : False
				},
			}
		},
	},
	"mappings": {
		"properties": {
				"search" : {
					"type" : "text",
					"analyzer" : "index_analyzer",
					"search_analyzer" : "search_analyzer",
					"similarity" : "boolean"
				},
				"title": {
					"type" : "text"
				},
				"summary" : {
					"type" : "text"
				},
				"_summary" : {
					"type" : "text"
				},
				"tables" : {
					"type" : "text"
				},
				"link" : {
					"type" : "keyword"
				},
				"published_datetime" : {
					"type" : "date",
				},
				"acquisition_datetime": {
					"type" : "date",
				},
				"authors" : {
					"type" : "keyword"
				},
				"article_source" : {
					"type" : "keyword"
				},
				"source" : {
					"type" : "keyword"
				},
				"tickers" : {
					"type" : "keyword"
				},
				"categories" : {
					"type" : "keyword"
				},
				"related" : {
					"type" : "keyword"
				},
				"_tickers" : {
					"type" : "keyword"
				},
				"sentiment": {
					"type" : "keyword"
				},
				"sentiment_score" : {
					"type" : "float"
				},
				"abs_sentiment_score" : {
					"type" : "float"
				}
			}
		}
	}

###################################################################################################

def download():

	if not CLEANDIR.exists():
		CLEANDIR.mkdir()

	for blob in CLEAN_BUCKET.list_blobs():

		print(blob.name)
		xz_file = CLEANDIR / blob.name
		blob.download_to_filename(xz_file)
		with tar.open(xz_file, "r:xz") as tar_file:
			tar_file.extractall(CLEANDIR)
		xz_file.unlink()

def index():

	es = Elasticsearch([f"{CONFIG['ES']['IP']}:{CONFIG['ES']['PORT']}"], timeout=60_000)
	# es = Elasticsearch(timeout=60_000)

	try:
		es.indices.delete("news")
	except Exception as e:
		print(e)

	es.indices.create("news", ES_MAPPINGS)

	items = []
	total_indexed, total_failed = 0, 0
	for i, file in enumerate(sorted(CLEANDIR.iterdir())):

		print("Processing:", file.name)
		with open(file, "r") as _file:
			items.extend(json.loads(_file.read()))

		if i > 0 and i % 20 == 0:

			print("Indexing", len(items))

			for item in items:
				if 'sentiment' not in item['_source']:
					print("Fault", file.name)

			indexed, failed = helpers.bulk(es,
										   items,
										   stats_only=True,
										   raise_on_error=False)

			print("Indexed:", indexed)
			print("Failed:", failed)

			total_indexed += indexed
			total_failed += failed

			items = []

	print("Final Indexing", len(items))
	if len(items) != 0:

		indexed, failed = helpers.bulk(es,
									   items,
									   stats_only=True,
									   raise_on_error=False)

		print("Final Indexed:", indexed)
		print("Final Failed:", failed)

		total_indexed += indexed
		total_failed += failed

	print("Total Indexed:", total_indexed)
	print("Total Failed:", total_failed)

def delete():

	for i, file in enumerate(sorted(CLEANDIR.iterdir())):

		print("Deleting", file)
		file.unlink()

if __name__ == '__main__':

	# download()
	# index()
	delete()
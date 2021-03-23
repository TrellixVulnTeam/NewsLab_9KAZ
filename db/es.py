from elasticsearch import Elasticsearch, helpers
from const import CONFIG, SUBSET
import json

###################################################################################################

ES_MAPPINGS = {
	"settings": {
		"number_of_shards": 1,
		"number_of_replicas": 0,
		"analysis" : {
			"analyzer" : {
				"search_analyzer" : {
					"tokenizer" : "classic",
					"filter" : [	
						"classic",
						"lowercase",
						"stop",
						"trim",
						"length_limiter",
						"porter_stem",
						"shingler",
						"unique"
					]
				}
			},
			"filter" : {
				"length_limiter" : {
					"type" : "length",
					"min" : 2
				},
				"shingler" : {
					"type" : "shingle",
					"min_shingle_size" : 2,
					"max_shingle_size" : 3
				},
			}
		},
		"similarity" : {
           	"no_length_norm" : {
               	"type" : "BM25",
               	"b" : 0
           	}
        }
	},
	"mappings": {
		"properties": {
				"search" : {
					"type" : "text",
					"analyzer" : "search_analyzer",
					"similarity" : "no_length_norm"
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

def index():

	es = Elasticsearch([f"{CONFIG['ES_IP']}:{CONFIG['ES_PORT']}"], timeout=60_000)
	# es = Elasticsearch(timeout=60_000)

	if not SUBSET:

		try:
			es.indices.delete("news")
		except Exception as e:
			print(e)

		es.indices.create("news", ES_MAPPINGS)

	files = list((RSS_FOLDER / "new").iterdir())
	files += list((CNBC_FOLDER / "new").iterdir())
	files += list((GOOGLE_FOLDER / "new").iterdir())

	items = []
	total_indexed, total_failed = 0, 0
	for i, file in enumerate(sorted(files)):

		print("Processing:", file.name)
		if "_" in file.name:
			sep, idx = "_", 2
		else:
			sep, idx = ".", 0

		if SUBSET and file.name.split(sep)[idx] not in SUBSET:
			continue

		with open(file, "r") as _file:
			items.extend(json.loads(_file.read()))

		if i > 0 and i % 20 == 0:

			print("Indexing", len(items))

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

if __name__ == '__main__':

	index()

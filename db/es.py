from elasticsearch import Elasticsearch, helpers
from const import CONFIG, SUBSET, CLEANDIR
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

def index():

	# es = Elasticsearch([f"{CONFIG['ES_IP']}:{CONFIG['ES_PORT']}"], timeout=60_000)
	es = Elasticsearch(timeout=60_000)

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

		if i > 0 and i % 40 == 0:

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

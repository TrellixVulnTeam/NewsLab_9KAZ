from const import CONFIG, CLEANDIR, CLEAN_BUCKET
from elasticsearch import Elasticsearch, helpers
import tarfile as tar
import json

###################################################################################################

ES_CLIENT = Elasticsearch(
	"http://{USER}:{KEY}@{HOST}:{PORT}".format(**CONFIG['ES_WRITER']),
	http_comprress=True,
	timeout=10000
)

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
				"search_analyzer_no_unigrams" : {
					"tokenizer" : "classic",
					"filter" : [	
						"classic",
						"lowercase",
						"stop",
						"trim",
						"porter_stem",
						"search_shingler_no_unigrams",
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
					"output_unigrams_if_no_shingles" : True,
					"output_unigrams" : True
				},
				"search_shingler_no_unigrams" : {
					"type" : "shingle",
					"min_shingle_size" : 2,
					"max_shingle_size" : 3,
					"output_unigrams_if_no_shingles" : False,
					"output_unigrams" : False
				},
				"search_shingler" : {
					"type" : "shingle",
					"min_shingle_size" : 2,
					"max_shingle_size" : 3,
					"output_unigrams_if_no_shingles" : True,
					"output_unigrams" : True
				},
			}
		},
	},
	"mappings": {
		"properties": {	
				"search" : {
					"type" : "text",
					"analyzer" : "index_analyzer",
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

CHUNKS = {
	"tweets": 80,
	"news": 20
}

###################################################################################################

def index():

	try:
		ES_CLIENT.indices.delete("news")
	except Exception as e:
		print(e)

	ES_CLIENT.indices.create("news", ES_MAPPINGS)

	items = []
	total_indexed, total_failed = 0, 0

	for folder in CLEANDIR.iterdir():

		if folder.name == '.gitignore': continue

		for i, file in enumerate(sorted(folder.iterdir())):

			if file.name == ".gitignore": continue

			print("Processing:", file.name)
			with open(file, "r") as _file:
				items.extend(json.loads(_file.read()))

			if i > 0 and i % CHUNKS[folder.name] == 0:

				print("Indexing", len(items))

				for item in items:
					if 'sentiment' not in item['_source']:
						print("Fault", file.name)

				indexed, failed = helpers.bulk(ES_CLIENT,
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

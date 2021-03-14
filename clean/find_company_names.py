from lists import TARGET_FALSE_PRES, TARGET_FALSE_POSTS
from nltk.tag import pos_tag
import pandas as pd
import time
import json

###################################################################################################

company_names = pd.read_csv("data/cleaned_company_names.csv")
company_names_dict = company_names.groupby('name')['ticker'].apply(list).to_dict()

exact_matches = company_names[company_names.type == 'none']
exact_matches_dict = exact_matches.groupby('name')['ticker'].apply(list).to_dict()

PUNCS = '!"#$%\'*+,-./:;<=>?@[\\]^_`{|}~–’‘'
APPOSTROPHES = "´’‘'’"

with open("data/items.json", "r") as file:
 	items = json.loads(file.read())

items = [
	{
		"_source" : {
			"title" : "Biotech stocks snapped back from their weekly losses and ended higher for the week ended March 12, with the broader market recovery partly aiding the reversal. Entera Bio Ltd. (NASDAQ:ENTX) was the biggest gainer of the week after it  disclosed  Phase 2 biomarker data that showed positive efficacy for EB613, its investigational drug being evaluated in postmenopausal women with osteoporosis. The week also witnessed a slew of  updates  from companies developing COVID-19 treatments and vaccines. AVEO Pharmaceuticals, Inc.  (AVEO) received the regulatory nod for its new drug application for Tivozanib, for difficult-to-treat kidney cancer that has spread from where it was originally formed. This culminates an eight-year-long wait from the initial filing date. IPO news flow returned, with Prometheus Biosciences, Inc. Common Stock (NASDAQ:RXDX), a biopharma focusing on therapies for inflammatory bowel disease, and Longboard Pharmaceuticals, Inc. Common Stock (NASDAQ:LBPH), a neurological diseases company, debuting on Wall Street. The two companies together raised a combined $270 million in gross proceeds from the initial public offerings. Here are the key catalysts for the unfolding week: Conferences The 15th International Conference on Alzheimer's and Parkinson's Diseases and Related Neurological Disorders: March 9-14 \n33rd Annual Virtual ROTH Conference: March 15-17 \nMuscular Dystrophy Association, or MDS, Virtual Clinical & Scientific Conference: March 15-18 \nMorgan Stanley Virtual Innovation in Pharma Week: March 15-19 \nMorgan Stanley Healthcare Corporate Access Days: March 16 \nOppenheimer 31st Annual Healthcare Conference (Virtual): March 16-18 \nThe Society of Gynecologic Oncology, or SGO, 2021 Annual Meeting: March 19-25 \nThe Endocrine Society's ENDO 2021: March 20-23 PDUFA Dates FibroGen Inc (NASDAQ:FGEN) and AstraZeneca plc's (NASDAQ:AZN) roxadustat PDUFA date of March 20 (Saturday) is likely to be extended, as the FDA has decided to hold an advisory committee meeting before deciding on the new drug application. FibroGen, the sponsor of the application, is seeking approval for the drug to treat anemia in chronic kidney disease patients. Clinical Trial Readouts/Presentations MDA Conference Presentations Sarepta Therapeutics, Inc. (NASDAQ:SRPT): Phase 1 data for SRP-9001 in Duchenne muscular dystrophy, and ... Full story available on Benzinga.com"
		}
	}
]

###################################################################################################

def preprocess_target(title, ns_title):

	title = f" {title} "
	tokens = title.split(" ")
	
	ns_title = f" {ns_title} "
	ns_tokens = ns_title.split(" ")
	
	if "Target" not in ns_tokens:
		return title, ns_title 
	
	idx = 0
	while True:
		
		try:
			idx = ns_tokens.index("Target", idx+1)
		except Exception as e:
			break
			
		if idx == 1:
			ns_tokens[idx] = "Target Corporation"
			tokens[idx] = ns_tokens[idx]
			continue
			
		pre, post = ns_tokens[idx - 1], ns_tokens[idx + 1]
		spre = tokens[idx - 1]
		
		if pre.lower() in TARGET_FALSE_PRES:
			continue
		if post.lower() in TARGET_FALSE_POSTS:
			continue
		if "'s" in spre:
			continue
			
		if pre == '':
			continue
			
		tag = pos_tag([pre])[0][1]

		if tag[:2] in ["JJ", "RB", "VB"]:
			continue
		is_year = tag == "CD" and len(pre) == 4 and "." not in pre
		if is_year and int(is_year) >= 2020:
			continue
		if tag == "NNS":
			continue
			
		ns_tokens[idx] = "Target Corporation"
		tokens[idx] = ns_tokens[idx]

	return ' '.join(tokens).strip(), ' '.join(ns_tokens).strip()

def preprocess_title(title):

	for app in APPOSTROPHES:
		repl = f"{app}s"
		title = title.replace(repl, f"'s")

	title = ' '.join([
		word.strip(PUNCS)
		for word in
		title.split(" ")
	])
	
	title = title.replace("  ", " ")
	ns_title = title.replace("'s", "")
	title, ns_title = preprocess_target(title, ns_title)

	titles = [f" {title.strip().lower()} "]
	if ns_title != title:
		titles.append(f" {ns_title.strip().lower()} ")

	return titles

###################################################################################################

def direct_match(titles):

	return [
		ticker
		for name in exact_matches_dict
		if any(
			f" {name} " in title
			for title in titles
		)
		for ticker in exact_matches_dict[name]
	]

def allgrams(tokens, min_n, max_n):

	def ngrams(tokens, n):
		return zip(*[tokens[i:] for i in range(n+1)])

	return [
		[
			' '.join(gram)
			for gram in ngrams(tokens, n)
		]
		for n in range(min_n - 1, max_n)
	]

def posgram_match(titles):

	tickers = {}
	offset = 0
	for title in titles:

		title = title.strip()
		grams = allgrams(title.split(" "), 1, 7)
		n = len(grams[0])

		i = 0
		while i < n:

			for j in range(min(7, n - i)):

				gram = grams[j][i]
				
				if gram in company_names_dict:
					
					if i in tickers and len(tickers[i]) <= j:
						tickers[i] = gram.split(" ")
					elif i not in tickers:
						tickers[i] = gram.split(" ")

			i += len(tickers.get(i, ['none']))

	_tickers = [
		ticker
		for match in tickers.values()
		for ticker in company_names_dict[' '.join(match)]
	]
	return list(set(_tickers))

def find_company_names(title):

	titles = preprocess_title(title)
	dm = direct_match(titles)
	pgm = posgram_match(titles)
	return list(set(dm + pgm))

###################################################################################################

if __name__ == '__main__':

	dm_ctr = 0
	pgm_ctr = 0
	ctr = 0

	stats = []
	time_stats = 0

	with open("matches.log", "w") as file:

		time_stats = time.time()

		for i, item in enumerate(items):

			titles = preprocess_title(item['_source']['title'])
			dm = direct_match(titles)
			pgm, matches = posgram_match(titles)

			if dm:
				dm_ctr += 1

			if pgm:
				pgm_ctr += 1

			if dm or pgm:
				ctr += 1

			log = f"""
				{titles[0]}
				{dm}
				{pgm},{matches}
			"""
			file.write(log)

			if i % 5000 == 0:
				print(i, (time.time() - time_stats)/(i+1))

			stats.append([titles[0], dm, pgm])

	print("DML", dm_ctr / len(items))
	print("PG", pgm_ctr / len(items))
	print("Net", ctr / len(items))

	df = pd.DataFrame(stats, columns = ['title', 'direct_lower', 'pgram'])
	df.to_csv("data/match.csv", index=False)

from const import DIR, logger
from lists import *
import pandas as pd
import inflect

###################################################################################################

stop_words = set(pd.read_csv(f"{DIR}/data/stop_words.csv").word.str.lower())
eng_words = set(pd.read_csv(f"{DIR}/data/english_words.csv").word.str.lower())
countries = set(pd.read_csv(f"{DIR}/data/countries.csv").word.str.lower())
currencies = set(pd.read_csv(f"{DIR}/data/currencies.csv").word.str.lower())
two_grams = set(pd.read_csv(f"{DIR}/data/two_grams.csv").gram.str.lower())
commodities = set(pd.read_csv(f"{DIR}/data/commodities.csv").commodity.str.lower())
inflect_engine = inflect.engine()

###################################################################################################

def remove_suffix(df):
	
	## Recursively remove suffixes in company names
	stats = []
	for vals in df.values:

		ticker, name, exch = vals

		stats.append([ticker, name, exch, 'none'])

		tokens = name.split(" ")
		while tokens[-1] in SUFFIXES:
			tokens = tokens[:-1]
			stats.append([ticker, ' '.join(tokens), exch, 'suff'])

	df = pd.DataFrame(stats, columns = ['ticker', 'name', 'exchange', 'type'])
	return df.drop_duplicates()

def replace_special_cases(df):
	
	df['name'] = df.name + " "

	new_names = list(df.name)
	new_names += list(df.name.str.replace("\\.com ", " ", regex=True))
	new_names += list(df.name.str.replace("'s ", "s "))
	new_names += list(df.name.str.replace(" & ", " and "))
	new_names += list(df.name.str.replace("-", " "))
	new_names += list(df.name.str.replace("-", ""))
	
	n = 7
	tickers = list(df.ticker)*n
	exchanges = list(df.exchange)*n
	types = list(df.type)*n
	
	df = pd.DataFrame(list(zip(new_names, tickers, exchanges, types)))
	df.columns = ['name', 'ticker', 'exchange', 'type']
	df['name'] = df.name.str.strip()

	return df

def remove_single_stop_and_english_words(df):
	
	words = df[df.name.isin(stop_words)]
	words = words[~words.name.isin(SAFE_SUFF_SINGLES)]
	df = df[~df.index.isin(words.index)]

	words = df[df.name.isin(eng_words)]
	words = words[~words.name.isin(SAFE_SUFF_SINGLES)]
	return df[~df.index.isin(words.index)]

###################################################################################################

def remove_modifiers(df):

	stats = []
	for row in df.values:

	    name, ticker, exchange, _type = row

	    stats.append([name, ticker, exchange, _type])
	    
	    tokens = name.split(" ")
	    while tokens[-1] in MODIFIERS and len(tokens) > 1:
	        tokens = tokens[:-1]
	        stats.append([' '.join(tokens), ticker, exchange, 'modifier'])

	df = pd.DataFrame(stats, columns = df.columns)
	return df.drop_duplicates()

def remove_modifier_stop_and_english_words(df):

	mods = df[df.type == 'modifier']
	mods = mods[mods.name.isin(stop_words)]
	df = df[~df.index.isin(mods.index)]

	mods = df[df.type == 'modifier']
	mods = mods[mods.name.str.split(" ").str.len() == 1]
	mods = mods[mods.name.isin(eng_words)]
	mods = mods[~mods.name.isin(SAFE_MOD_SINGLES)]
	return df[~df.index.isin(mods.index)]

def remove_modifier_duplicates(company_names):

	def remove_duplicates(df, length):

		mods = df[df.type == 'modifier']
		mods = mods[mods.name.str.split(" ").str.len() == length]

		vcs = mods.name.value_counts()
		singles = vcs[vcs > 1].index
		banned_singles = []

		for single in singles:
		
		    tickers = mods[mods.name == single].ticker
		    names = df[df.ticker.isin(tickers)]
		    doubles = names[names.name.str.split(" ").str.len() == length + 1]
		
		    if doubles.name.nunique() > 1:
		        banned_singles.append(single)

		return df[~df.name.isin(banned_singles)]

	company_names = remove_duplicates(company_names, 1)
	return remove_duplicates(company_names, 2)

###################################################################################################

def replace_synonyms(df):
	
	df['name'] = df.name.str.strip() + " "
	df1 = df.copy()
	df2 = df.copy()
	
	for syn in SYNONYMS:
		df1['name'] = df1.name.str.replace(*syn)    
	for syn in SYNONYMS:
		df2['name'] = df2.name.str.replace(*syn[::-1])

	df['name'] = df.name.str.strip()
	df1['name'] = df1.name.str.strip()
	df2['name'] = df2.name.str.strip()
	
	return pd.concat([df, df1, df2]).drop_duplicates()

def replace_all_synonyms(df):
	
	while True:
		new_df = replace_synonyms(df)
		logger.info(f"Synonym Replacement. Old:{len(df)} New:{len(new_df)}")
		if len(new_df) == len(df):
			break
		df = new_df.copy()
		
	return df

def remove_short_names(df):

	short_names = df[df.name.str.len() == 2]
	short_names = short_names[~short_names.name.isin(SAFE_SUFF_SINGLES)]
	return df[~df.index.isin(short_names.index)].reset_index(drop=True)

def remove_all_number_names(df):

	return df[~df.name.str.isnumeric()].reset_index(drop=True)

def replace_indices(df):
	
	new = [
		['dow', 'DIA', 'NASDAQ', 'suff'],
		['the dow', 'DIA', 'NASDAQ', 'suff'],
		['dow jones industrial average', 'DIA', 'NASDAQ', 'none'],
		['dow jones', 'DIA', 'NASDAQ', 'none'],
		['nasdaq composite', 'QQQ', 'NASDAQ', 'none'],
		['nasdaq', 'QQQ', 'NASDAQ', 'suff'],
		['the nasdaq', 'QQQ', 'NASDAQ', 'suff'],
		['the s&p', 'SPY', 'NASDAQ', 'suff'],
		['russell 2000', 'IWM', 'NASDAQ', 'suff'],
		['the russell', 'IWM', 'NASDAQ', 'suff'],
	]
	
	df = df[df.name != 'dow']
	return pd.concat([
		df,
		pd.DataFrame(new, columns = df.columns)
	]).reset_index(drop=True)

def remove_countries_and_currencies(df):

	df = df[~df.name.isin(countries)]
	df = df[~df.name.isin(currencies)]
	return df

def remove_english_two_grams(df):

	names = df[df.name.isin(two_grams)].name
	to_remove = []

	for name in names:

		if name in SAFE_TWO_GRAMS:
			continue

		w1, w2 = name.split(" ")
		if w2 in SUFFIXES:
			continue

		to_remove.append(name)
		to_remove.append(inflect_engine.plural(name))
		
		sw2 = inflect_engine.singular_noun(name)
		if sw2:
			to_remove.append(sw2)

	return df[~df.name.isin(to_remove)]

def remove_commodities(df):

	return df[~df.name.isin(commodities)]

def add_nicknames(df):

	nicknames = [
		['google', 'GOOG', 'NASDAQ', 'custom'],
		['jpm', 'JPM', 'NYSE', 'custom'],
		['j&j', 'JNJ', 'NYSE', 'custom'],
		['jnj', 'JNJ', 'NYSE', 'custom'],
		['walmart', 'WMT', 'NYSE', 'custom'],
		['wal-mart', 'WMT', 'NYSE', 'custom'],
		['disney', 'DIS', 'NYSE', 'custom'],
		['p&g', 'PG', 'NYSE', 'custom'],
		['exxon', 'XOM', 'NYSE', 'custom'],
		['toyota', 'TM', 'NYSE', 'custom'],
		['tmobile', 'TMUS', 'NASDAQ', 'custom'],
		['citi', 'C', 'NYSE', 'custom'],
		['costco', 'COST', 'NASDAQ', 'custom'],
		['charles schwab', 'SCHW', 'NYSE', 'custom'],
		['sanofi', 'SNY', 'NASDAQ', 'custom'],
		['amex', 'AXP', 'NYSE', 'custom'],
		['ge', 'GE', 'NYSE', 'custom'],
		['ibm', 'IBM', 'NYSE', 'custom'],
		['zoom', 'ZM', 'NASDAQ', 'custom'],
		['amd', 'AMD', 'NASDAQ', 'custom'],
		['advanced micro devices', 'AMD', 'NASDAQ', 'custom'],
		['glaxo smith skline', 'GSK', 'NASDAQ', 'custom'],
		['snapchat', 'SNAP', 'NYSE', 'custom'],
		['colgate', 'CL', 'NYSE', 'custom'],
		['snapchat', 'SNAP', 'NYSE', 'custom'],
		['ford', 'F', 'NYSE', 'custom'],
		['kraft', 'KHC', 'NASDAQ', 'custom'],
		['lululemon', 'LULU', 'NASDAQ', 'custom'],
		['motorola', 'MSI', 'NYSE', 'custom'],
		['merril lynch', 'IPB', 'AMEX', 'custom'],
		['j.p morgan', 'JPM', 'NYSE', 'custom'],
		['bitcoin', 'BTCC', 'TSX', 'custom'],
		['bitcoin', 'BTC', 'NASDAQ', 'custom'],
		['etherum', 'ETH', 'NASDAQ', 'custom'],
		['ether', 'ETH', 'NASDAQ', 'custom'],
		['goldman sachs', 'GS', 'NYSE', 'custom'],
		['goldman', 'GS', 'NYSE', 'custom'],
	]
	df = pd.concat([
		df,
		pd.DataFrame(nicknames, columns = df.columns)
	])
	return df

def curate_company_names(company_names):

	company_names['name'] = company_names.name.str.lower()
	logger.info(f"Initial {company_names.shape}")

	## Suffix Section
	company_names = remove_suffix(company_names)
	logger.info(f"Remove Suffix {company_names.shape}")

	company_names = replace_special_cases(company_names)
	logger.info(f"Special Cases {company_names.shape}")

	company_names = remove_single_stop_and_english_words(company_names)
	logger.info(f"Single stop and english {company_names.shape}")

	## Modifier Section

	company_names = remove_modifiers(company_names)
	logger.info(f"Modifiers {company_names.shape}")

	company_names = remove_modifier_stop_and_english_words(company_names)
	logger.info(f"Modifier stop and english {company_names.shape}")

	company_names = remove_modifier_duplicates(company_names)
	logger.info(f"Modifier Duplicates {company_names.shape}")

	## Final cleaning
	exchanges = ["AMEX", "NASDAQ", "NYSE", "TSX", "LSE"]
	company_names = company_names[company_names.exchange.isin(exchanges)]
	logger.info(f"Exchange Filter {company_names.shape}")

	company_names = replace_all_synonyms(company_names)
	logger.info(f"Replace Synonyms {company_names.shape}")

	company_names = replace_indices(company_names)
	logger.info(f"Replace Indices {company_names.shape}")

	company_names = remove_all_number_names(company_names)
	logger.info(f"Remove Numbered Names {company_names.shape}")

	company_names = remove_short_names(company_names)
	logger.info(f"Remove Short Names {company_names.shape}")

	company_names = company_names[~company_names.name.isin(MANUAL_OVERRIDES)]
	logger.info(f"Remove Manual Overrides {company_names.shape}")

	company_names = remove_countries_and_currencies(company_names)
	logger.info(f"Remove Countries and Currencies {company_names.shape}")

	company_names = remove_english_two_grams(company_names)
	logger.info(f"Remove English Two Grams {company_names.shape}")

	company_names = remove_commodities(company_names)
	logger.info(f"Remove Commodities {company_names.shape}")

	## Super special case of 'Target'. Add nicknames
	company_names = company_names[company_names.name != 'target']
	logger.info(f"Remove Target {company_names.shape}")

	company_names = add_nicknames(company_names)
	logger.info(f"Added Nicknames {company_names.shape}")

	## Sort and Save
	company_names = company_names.sort_values('ticker')
	company_names = company_names.drop_duplicates().reset_index(drop=True)
	logger.info(f"Drop dupes {company_names.shape}")

	return company_names

###################################################################################################
	
if __name__ == '__main__':

	company_names = pd.read_csv(f"{DIR}/data/company_names.csv")
	company_names = curate_company_names(company_names)
	company_names.to_csv(f"{DIR}/data/curated_company_names.csv", index=False)

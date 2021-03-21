from selenium.webdriver.support.ui import Select
from const import CONFIG, DIR, logger
from selenium import webdriver
import chromedriver_binary
from pathlib import Path
import pandas as pd
import traceback
import sys, os
import time

sys.path.append(f"{DIR}/..")
from utils import send_metric

###################################################################################################

EXCHANGES = [
	"AMEX",
	"LSE",
	"NASDAQ",
	"NYSE",
	"TSX",
	"TSXV"
]

DATA = Path(f"{DIR}/name_data")

###################################################################################################

def download_company_names():

	for file in DATA.iterdir():
		if file.name == '.gitignore':
			continue
		file.unlink()

	###############################################################################################

	options = webdriver.ChromeOptions()
	options.add_argument("--headless")
	options.add_argument("--disable-gpu")
	options.add_argument("window-size=1024,768")
	options.add_argument("--no-sandbox")
	options.add_experimental_option("prefs", {
		"download.default_directory" : str(DATA)
	})

	driver = webdriver.Chrome(options = options)

	logger.info("Getting web page...")
	driver.get("http://eoddata.com")

	username_input = driver.find_element_by_id("ctl00_cph1_lg1_txtEmail")
	password_input = driver.find_element_by_id("ctl00_cph1_lg1_txtPassword")
	login_button = driver.find_element_by_id("ctl00_cph1_lg1_btnLogin")

	username_input.send_keys("zQuantz")
	password_input.send_keys("Street101!")
	
	logger.info("Logging in...")
	login_button.click()

	logger.info("Getting download page...")
	driver.get("http://eoddata.com/symbols.aspx")

	for exchange in EXCHANGES:

		logger.info(f"Downloading: {exchange}")

		exchange_selector = Select(driver.find_element_by_id("ctl00_cph1_cboExchange"))
		exchange_selector.select_by_value(exchange)
		
		time.sleep(5)

		download_button = driver.find_element_by_id("ctl00_cph1_ch1_divLink")
		download_button = download_button.find_element_by_tag_name("a")
		download_button.click()

		time.sleep(5)

	###############################################################################################

	df = []
	for file in DATA.iterdir():
		if file.name == '.gitignore':
			continue
		df.append(pd.read_csv(file, delimiter="\t"))
		df[-1]['exchange'] = file.name[:-4]

	df = pd.concat(df)
	df.columns = ['ticker', 'name', 'exchange']
	df = df.sort_values('ticker').reset_index(drop=True)

	###############################################################################################

	df = df[df.ticker.str.len() <= 6]

	combo = df.name + " " + df.exchange
	vcs = combo.value_counts()    
	df = df[combo.isin(vcs[vcs == 1].index)]
	df = df[df.ticker.str.count("\\.") <= 1]

	ndaq = df[df.exchange == 'NASDAQ']
	ndaq = ndaq[ndaq.ticker.str.len() > 4]
	mods = ndaq.ticker.str[-1]
	mods = ndaq[~mods.isin(['A', 'B', 'C'])]
	df = df[~df.index.isin(mods.index)]

	ticker_mods = df[df.ticker.str.count("\\.") == 1]
	mod = ticker_mods.ticker.str.split("\\.").str[-1]
	ticker_mods = ticker_mods[~mod.isin(["A", "B", "C"])]
	df = df[~df.index.isin(ticker_mods.index)]

	df = df[~df.ticker.str[0].str.isnumeric()]
	df = df[~df.ticker.str[-1].str.isnumeric()]

	mods = df[df.ticker.str.count("-") == 1].ticker
	mods = mods[~mods.str[-1].isin(['A', 'B', 'C'])]
	df = df[~df.index.isin(mods.index)]

	return df

if __name__ == '__main__':

	df = download_company_names()
	df.to_csv(f"{DIR}/data/company_names.csv", index=False)

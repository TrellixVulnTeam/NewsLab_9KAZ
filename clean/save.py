from datetime import datetime, timedelta
from const import DIR, CONFIG, logger
from traceback import format_exc
from socket import gethostname
from pathlib import Path
import sys, os

sys.path.append(f"{DIR}/..")
from utils import send_to_bucket, send_metric, save_items

def check_file(file, now):
	ctime = file.stat().st_ctime
	delta = (now - datetime.fromtimestamp(ctime))
	return int(delta.seconds / 60) > delay

if __name__ == '__main__':

	if gethostname() != CONFIG['MACHINE']['HOSTNAME']:
		CONFIG['GCP']['CLEAN_BUCKET'] = "tmp_items"

	try:

		filedate = datetime.now() - timedelta(days = 1)
		filedate = filedate.strftime('%Y-%m-%d')

		path = Path(f"{DIR}/clean_news_data")
		xz_file = Path(f"{DIR}/clean_news_data_backup/{filedate}.tar.xz")

		n_items, n_unique = save_items(path, set(), filedate)
		send_metric(CONFIG, "clean_count", "int64_value", n_items)
		send_metric(CONFIG, "unique_clean_count", "int64_value", n_unique)

		send_to_bucket(
			CONFIG['GCP']['CLEAN_BUCKET'],
			'clean',
			xz_file,
			logger=logger
		)

		logger.info(f"RSS save successeful.")
		send_metric(CONFIG, "clean_save_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.warning(f"RSS save failed. {e}, {format_exc()}")
		send_metric(CONFIG, "clean_save_success_indicator", "int64_value", 0)

from datetime import datetime, timedelta
from const import SDATE, DIR, CONFIG, logger
from traceback import format_exc
from socket import gethostname
from pathlib import Path
import sys, os

sys.path.append(f"{DIR}/..")
from utils import send_to_bucket, send_metric, save_items

def check_file(file, now):
	ctime = file.stat().st_ctime
	delta = (now - datetime.fromtimestamp(ctime))
	return int(delta.seconds / 60) > 3

if __name__ == '__main__':

	if gethostname() != CONFIG['MACHINE']['HOSTNAME']:
		CONFIG['GCP']['CLEAN_BUCKET'] = "tmp_items"

	try:

		path = Path(f"{DIR}/clean_data")
		xz_file = Path(f"{DIR}/clean_data_backup/{SDATE}.tar.xz")

		raw_path = Path(f"{DIR}/news_data")
		files = list(raw_path.iterdir())
		files.remove(raw_path / ".gitignore")

		now = datetime.now()
		[
			file.unlink()
			for file in files
			if check_file(file, now)
		]

		n_items, n_unique = save_items(path, SDATE)
		send_metric(CONFIG, "clean_count", "int64_value", n_items)
		send_metric(CONFIG, "unique_clean_count", "int64_value", n_unique)

		send_to_bucket(
			CONFIG['GCP']['CLEAN_BUCKET'],
			'news',
			xz_file,
			logger=logger
		)

		logger.info(f"RSS save successeful.")
		send_metric(CONFIG, "clean_save_success_indicator", "int64_value", 1)

	except Exception as e:

		logger.warning(f"RSS save failed. {e}, {format_exc()}")
		send_metric(CONFIG, "clean_save_success_indicator", "int64_value", 0)

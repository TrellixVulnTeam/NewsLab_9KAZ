from google.oauth2.service_account import Credentials
from google.cloud import monitoring_v3
from google.cloud import storage
from mailjet_rest import Client
from datetime import datetime
from hashlib import md5
import tarfile as tar
import requests
import base64
import sys,os
import json
import time

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))

with open(f"{DIR}/news_config.json", "r") as file:
	CONFIG = json.loads(file.read())

CREDS = Credentials.from_service_account_file(os.environ.get(CONFIG['GCP']['ENV_CREDS_KEY']))
METRIC_CLIENT = monitoring_v3.MetricServiceClient(credentials=CREDS)
STORAGE_CLIENT = storage.Client(credentials=CREDS)

###################################################################################################

class DummyLogger():

	def warning(self, str_):
		print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} - {str_}")

	def info(self, str_):
		print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} - {str_}")

def send_to_bucket(bucket_name, bucket_prefix, file, logger=None):

	if not logger:
		logger = DummyLogger()

	max_tries = 5
	storage_attempts = 0

	while storage_attempts < max_tries:

		try:

			bucket = STORAGE_CLIENT.bucket(bucket_name)
			blob = bucket.blob(f"{bucket_prefix}/{file.name}")
			blob.upload_from_filename(file, checksum="md5")
			
			logger.info(f"Upload to {bucket_name}/{bucket_prefix} successful. {storage_attempts} attempts.")

			break

		except Exception as e:

			logger.warning(f"Upload to {bucket_name}/{bucket_prefix} failed. {storage_attempts} attempts.")
			storage_attempts += 1

	if storage_attempts >= max_tries:
		raise Exception("Too Many Storage Attempts.")

def send_metric(CONFIG, metric_name, metric_type, metric):

	series = monitoring_v3.types.TimeSeries()

	series.metric.type = f"custom.googleapis.com/{metric_name}"
	series.resource.type = 'global'

	point = series.points.add()
	setattr(point.value, metric_type, metric)

	now = time.time()
	point.interval.end_time.seconds = int(now)
	point.interval.end_time.nanos = int(
		(now - point.interval.end_time.seconds) * 10**9)

	project_name = METRIC_CLIENT.project_path(CONFIG['GCP']['PROJECT_ID'])
	METRIC_CLIENT.create_time_series(project_name, [series])

def delete_gcp_metric(metric_name):

	project_id = CONFIG['GCP']['PROJECT_ID']
	name = f"projects/{project_id}/metricDescriptors/custom.googleapis.com/{metric_name}"	

	METRIC_CLIENT.delete_metric_descriptor(name)
	print('Deleted metric descriptor {}.'.format(name))

def create_gcp_metric(metric_name, value_type):

	project_name = METRIC_CLIENT.project_path(CONFIG['GCP']['PROJECT_ID'])

	descriptor = monitoring_v3.types.MetricDescriptor()
	descriptor.type = f'custom.googleapis.com/{metric_name}'

	descriptor.metric_kind = (monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
	descriptor.value_type = (monitoring_v3.enums.MetricDescriptor.ValueType[value_type])

	descriptor.description = 'This is a simple example of a custom metric.'
	descriptor = METRIC_CLIENT.create_metric_descriptor(project_name, descriptor)

	print('Created {}.'.format(descriptor.name))

###################################################################################################

def encode_text(filename, filepath):

	with open(f"{filepath}/{filename}", "r") as file:
		content = file.read()

	content = content.encode()
	content = base64.b64encode(content)
	content = content.decode()

	return {
		"ContentType" : "text/plain",
		"Filename" : filename,
		"Base64Content" : content
	}

def encode_zip(filename, filepath):

	with open(f"{filepath}/{filename}.b64", "wb+") as b64_file:
		with open(f"{filepath}/{filename}", "rb") as zip_file:
			base64.encode(zip_file, b64_file)
		b64_file.seek(0)
		content = b64_file.read()
	content = content.decode()
	
	os.remove(f"{filepath}/{filename}.b64")

	return {
		"ContentType" : "application/zip",
		"Filename" : filename,
		"Base64Content" : content
	}

def send_email(CONFIG, subject, body, attachments, logger=None):

	if not logger:
		logger = DummyLogger()

	max_tries = 5
	email_attempts = 0

	while email_attempts < max_tries:

		try:

			api_public_key = CONFIG['MAILJET']['PUBKEY']
			api_private_key = CONFIG['MAILJET']['PRIVKEY']
			
			client = Client(auth=(api_public_key, api_private_key), version='v3.1')

			b64_attachments = []
			for attachment in attachments:
				
				filename = attachment['filename']
				filepath = attachment['filepath']

				if attachment['ContentType'] == "plain/text":
					
					b64_attachments.append(encode_text(filename, filepath))

				elif attachment['ContentType'] == "application/zip":
					
					encoded_zip = encode_zip(filename, filepath)
					
					filesize = sys.getsizeof(encoded_zip['Base64Content'])
					filesize /= 1_000_000

					if filesize < 15:
						b64_attachments.append(encoded_zip)

			data = {
				"Messages" : [
					{
						"From" : CONFIG['MAILJET']['SENDER'],
						"To" : CONFIG['MAILJET']['RECIPIENTS'],
						"Subject" : subject,
						"HTMLPart" : body,
						"Attachments" : b64_attachments
					},
				]
			}
			
			result = client.send.create(data=data)
			status, response = result.status_code, result.json()

			if status == 200:
				break
			else:
				logger.warning(f"Email Status Error. {status} - {response}")

		except Exception as e:

			logger.warning(f"Emailing Attempt Error. {e}")

		email_attempts += 1

	if email_attempts >= max_tries:
		raise Exception("Emailing Failure.")

###################################################################################################

def request(url, logger=None):

	if not logger:
		logger = DummyLogger()

	max_tries = 5
	tries = 0

	while tries < max_tries:

		try:

			response = requests.get(url, timeout=30)
			return response

		except Exception as e:

			logger.warning(e)
			tries += 1
			time.sleep(tries)

	if tries >= max_tries:
		raise Exception("Too many requests.")

def save_items(path, ids, date):

	files = list(path.iterdir())
	files.remove(path / ".gitignore")

	items = []
	n_items = 0
	for file in files:
		
		with open(file, "r") as _file:

			for item in json.loads(_file.read()):

				n_items += 1
				_id = item['_id']
				if _id in ids:
					continue

				ids.add(_id)
				items.append(item)

	###############################################################################################

	json_file = path.parent / f'{path.name}_backup'
	json_file = json_file / f'{date}.json'
	xz_file = json_file.with_suffix(".tar.xz")

	with open(json_file, "w") as file:
		file.write(json.dumps(items))

	with tar.open(xz_file, "x:xz") as tar_file:
		tar_file.add(json_file, arcname=json_file.name)

	json_file.unlink()

	for file in files:
		file.unlink()

	return n_items, len(items)

if __name__ == '__main__':

	# bucket_backup()

	# RSS
	# create_gcp_metric("rss_count", "INT64")
	# create_gcp_metric("rss_counter", "INT64")
	# create_gcp_metric("unique_rss_count", "INT64")
	# create_gcp_metric("rss_save_success_indicator", "INT64")

	# Clean
	# create_gcp_metric("clean_count", "INT64")
	# create_gcp_metric("unique_clean_count", "INT64")
	# create_gcp_metric("clean_save_success_indicator", "INT64")

	# News
	# create_gcp_metric("news_count", "INT64")
	# create_gcp_metric("unique_news_count", "INT64")
	# create_gcp_metric("news_success_indicator", "INT64")

	# General
	# create_gcp_metric("company_names_success_indicator", "INT64")

	pass

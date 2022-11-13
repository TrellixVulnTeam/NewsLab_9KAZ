from apscheduler.schedulers.background import BlockingScheduler
from datetime import datetime, timezone
from collections import deque
from threading import Thread

from hashlib import md5
from const import DIR
import feedparser
import socket
import sys, os
import json
import uuid

###################################################################################################

KEYS = [
	'links',
	'link',
	'updated',
	'published',
	'published_parsed',
	'id'
]

def get_id(item):

	for key in KEYS:
		if key in item:
			item.pop(key)

	_hash = json.dumps(item, sort_keys = True).encode()
	return md5(_hash).hexdigest()

###################################################################################################

class Feeds(Thread):
	
	WINDOW = 10_000

	def __init__(self, sources, feeds, sleep, logger):

		Thread.__init__(self)

		self.sleep = sleep
		self.logger = logger
		
		self.coords = deque([
			(source.strip(), feed.strip())
			for source, feed in zip(sources, feeds)
		])

		self.entries = []
		self.last = {
			feed : []
			for _, feed in self.coords
		}

		socket.setdefaulttimeout(3)

	def run(self):

		job_defaults = {
			'coalesce': True,
			'max_instances': 1
		}
		self.blocker = BlockingScheduler(job_defaults = job_defaults)
		self.blocker.add_job(self.parse_feed, 'cron', second=f'*/{self.sleep}', id='parse_feed')
		
		self.blocker.start()

	def on_close(self):

		self.blocker.shutdown()
		self.join()
		
	def parse_feed(self):

		self.coords.rotate()
		if len(self.coords) == 0:
			return

		self.source, self.feed = self.coords[0]

		try:
			response = feedparser.parse(self.feed)
		except Exception as e:
			self.logger.warning(f"Status,{self.source},{self.feed},{e}")
			return

		status = response.get('status', None)
		if not status:
			self.logger.warning(f"Status,{self.source},{self.feed},None")
			return

		if status != 200:
			self.logger.warning(f"Status,{self.source},{self.feed},{status}")
			return
		
		entries = response.get('entries', None)
		if not entries:
			self.logger.warning(f"Entries,{self.source},{self.feed},None")
			return

		for entry in entries:
			
			_id = entry['id'] if self.source == 'Google' else get_id(entry.copy())
			if _id in self.last[self.feed]:
				continue

			self.last[self.feed].append(_id)
			self.last[self.feed] = self.last[self.feed][-self.WINDOW:]

			entry['acquisition_datetime'] = datetime.utcnow().isoformat()[:19]
			entry['feed_source'] = self.source
			entry['_source'] = 'rss'
			entry['_id'] = _id

			self.entries.append(entry)

		if len(self.entries) > 0:

			with open(f"{DIR}/news_data/{str(uuid.uuid4())}.json", "w") as file:
				file.write(json.dumps(self.entries))

			self.entries = []

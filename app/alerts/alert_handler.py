import csv
import logging
import os
import pytz

from alerts.check_limit_handler import CheckLimitHandler
from common.constants import CONFIG_ALERT_FILENAME
from common.constants import DATETIME_FORMAT
from common.constants import TIMEOUT_WAITING_MESSAGE
from datetime import datetime, timedelta
from queue import Empty
from multiprocessing import Process, Queue


CONFIG_ALERT_FILENAME = "../data/config_alerts.csv"

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

class AlertHandler(Process):
	def __init__(self, queue_size, stop_event, timer_event, time_alert, n_workers):
		super(AlertHandler, self).__init__()
		
		self._stop_event = stop_event
		self._timer_event = timer_event
		self._time_alert = time_alert

		self._queue_alert_to_log = Queue(maxsize=queue_size)
		self._queue_alert_to_check = Queue(maxsize=queue_size)

		self._response_handler = Process(target=self.__log_alerts)
		self._check_limit_handlers = [CheckLimitHandler(self._queue_alert_to_check, self._queue_alert_to_log, self._stop_event) for i in range(n_workers)]
		
		self._response_handler.start()

		for w in self._check_limit_handlers:
			w.start()

	def run(self):
		while not self._stop_event.is_set():
			while not self._timer_event.wait(self._time_alert):
				try:
					self.__read_set_alerts()

				except Exception as e:
					logging.error(f"[ALERT HANDLER] Run: {e}")

		self.__join()

	def __join(self):
		self._queue_alert_to_log.join()

		self._queue_alert_to_check.join()

		for c in self._check_limit_handlers:
			c.join()

		self._response_handler.join()


	def __read_set_alerts(self):
		filename = os.path.join(ROOT_DIR, CONFIG_ALERT_FILENAME)
		with open(filename, "r") as _file:
			rows = csv.DictReader(_file, fieldnames=["metric_id", "limit", "aggregation", "aggregation_window_secs"])
			next(rows)
			for alert in rows:
				alert = self.__add_alerts_datetime(alert)
				self._queue_alert_to_check.put(alert)


	def __add_alerts_datetime(self, alert):
		tz = pytz.timezone('America/Argentina/Buenos_Aires')
		now = datetime.now(tz)
		alert["from_date"] = (now - timedelta(seconds=self._time_alert)).strftime(DATETIME_FORMAT)
		alert["to_date"] = now.strftime(DATETIME_FORMAT)
		return alert

	def __log_alerts(self):
		while not self._stop_event.is_set():
			try:
				response = self._queue_alert_to_log.get(timeout=TIMEOUT_WAITING_MESSAGE)
				logging.info(f"[LOG_ALERT] Get from queue: {response}")
			except Empty:
				continue

from threading import Thread
import logging
from queue import Queue, Empty
from common.constants import CONFIG_ALERT_FILENAME, TIMEOUT_WAITING_MESSAGE
from alerts.check_limit_handler import CheckLimitHandler
import csv



class AlertHandler(Thread):
	def __init__(self, queue_size, stop_event, timer_event, time_alert):
		Thread.__init__(self)
		self._stop_event = stop_event
		self._timer_event = timer_event
		self._time_alert = time_alert

		self._queue_alert_to_log = Queue(maxsize=queue_size)
		self._response_thread = Thread(target=self.__log_alerts)
		self._response_thread.start()
		self._checkers = []


	def run(self):
		while not self._stop_event.is_set():
			while not self._timer_event.wait(self._time_alert):
				try:
					self.__read_set_alerts()

				except Exception as e:
					logging.error(f"[ALERT HANDLER] {e}")

				self.__join()

	def __join(self):
		print("START: ", len(self._checkers))
		for c in self._checkers:
			if c.has_end():
				c.join()
				self._checkers.remove(c)

		print("END", len(self._checkers))


	def __read_set_alerts(self):
		with open(CONFIG_ALERT_FILENAME, "r") as _file:
			rows = csv.DictReader(_file, fieldnames=["metric_id", "limit", "aggregation", "aggregation_window_secs"])
			next(rows)
			for alert in rows:
				checker = CheckLimitHandler(alert, self._queue_alert_to_log, self._stop_event)
				checker.start()
				self._checkers.append(checker)


	def __log_alerts(self):
		while not self._stop_event.is_set():
			try:
				response = self._queue_alert_to_log.get(timeout=TIMEOUT_WAITING_MESSAGE)
				self._queue_alert_to_log.task_done()
				logging.info(f"[LOG_ALERT] Get from queue: {response}")
			except Empty:
				continue

		self._queue_alert_to_log.join()
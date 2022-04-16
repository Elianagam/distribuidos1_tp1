from threading import Thread
import logging
from queue import Queue, Empty
from common.constants import TIME_ALERT, CONFIG_ALERT_FILENAME, TIMEOUT_WAITING_MESSAGE



class AlertHandler(Thread):
	def __init__(self, queue_size, stop_event, timer_event):
		Thread.__init__(self)
		self._stop_event = stop_event
		self._timer_event = timer_event

		self._queue_alert_to_log = Queue(maxsize=queue_size)
		self._response_thread = threading.Thread(target=self.__log_alerts)
		self._response_thread.start()
		self._checkers = []


	def run():
		while not self._stop_event.is_set():
			while not self._timer_event.wait(TIME_ALERT):
				try:
					alerts_set = self.__read_set_alerts()
					for alert in alerts_set:
						checker = CheckLimitHandler(alert, self._queue_alert_to_log, self._stop_event)
						checker.start()

				except Exception as e:
					logging.errr(f"[ALERT HANDLER] {e}")

			for i in range(self._checkers):
				if self._checkers[i].has_end():
					self._checkers[i].join()
					del self._checkers[i]



	def __read_set_alerts(self):
		with open(CONFIG_ALERT_FILENAME, "r") as _file:
			rows = csv.DictReader(_file, fieldnames=["metric_id", "limit", "aggregation", "aggregation_window_secs"])
			return rows


	def __log_alerts(self):
		while not self._stop_event.is_set():
			try:
				response = self._queue_alert_to_log.get(timeout=TIMEOUT_WAITING_MESSAGE)
				self._queue_alert_to_log.task_done()
				logging.info(f"[LOG_ALERT] {response}")
			except Empty:
				continue

		self._queue_alert_to_log.join()
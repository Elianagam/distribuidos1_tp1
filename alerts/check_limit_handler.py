from threading import Thread
import logging
from metric_file_handler import MetricFileHandler


class CheckLimitHandler(Thread):
	def __init__(self, alert, queue_alert_to_log, stop_event):
		self._alert = alert
		self._queue_alert_to_log = queue_alert_to_log
		self._stop_event = stop_event
		self._end = False
		self._metrics_file = MetricFileHandler()

	def has_end(self):
		return self._end

	def run(self):
		while not self._stop_event.is_set():
			try:
				agg = self._metrics_file.check_limit(self._alert)
				if agg != None:
					self._queue_alert_to_log.put(agg)
					self._queue_alert_to_log.task_done()
					self._end = True
			except Exception as e:
				logging.error(f"[CHECK_LIMIT_HANDLER] Error {e}")
	
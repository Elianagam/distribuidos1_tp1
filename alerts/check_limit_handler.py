from threading import Thread, Event
import logging
from server.metric_file_handler import MetricFileHandler


class CheckLimitHandler(Thread):
	def __init__(self, alert, queue_alert_to_log, stop_event):
		Thread.__init__(self)
		self._alert = alert
		self._queue_alert_to_log = queue_alert_to_log
		self._stop_event = stop_event
		self._finish = Event()
		self._metrics_file = MetricFileHandler()

	def has_end(self):
		return self._finish.is_set()

	def run(self):
		try:
			if not self._stop_event.is_set() and not self._finish.is_set():
				agg = self._metrics_file.check_limit(self._alert)
				if agg != None:
					self._queue_alert_to_log.put(agg)
					self._finish.set()
					logging.info(f"[CHECK_LIMIT_HANDLER] Limit check for metric [{self._alert['metric_id']}] has end.")
		except Exception as e:
			logging.error(f"[CHECK_LIMIT_HANDLER] Error run: {e}")
	
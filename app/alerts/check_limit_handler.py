import logging

from common.constants import TIMEOUT_WAITING_MESSAGE
from queue import Empty
from threading import Thread
from metric_file_handler import MetricFileHandler



class CheckLimitHandler(Thread):
	def __init__(self, queue_alert_to_check, queue_alert_to_log, stop_event):
		Thread.__init__(self)
		self._queue_alert_to_log = queue_alert_to_log
		self._queue_alert_to_check = queue_alert_to_check
		self._stop_event = stop_event
		self._metrics_file = MetricFileHandler()

	def run(self):
		while not self._stop_event.is_set():
			try:
				alert = self._queue_alert_to_check.get(timeout=TIMEOUT_WAITING_MESSAGE)
				logging.debug(f"[CHECK_LIMIT_HANDLER] Ćheck limit for alert: {alert}")
				self._queue_alert_to_check.task_done()
				agg_alert = self._metrics_file.check_limit(alert)

				if agg_alert != None:
					self._queue_alert_to_log.put(agg_alert)
					logging.info(f"[CHECK_LIMIT_HANDLER] Limit check for metric [{alert['metric_id']}] has end.")
			
			except Empty:
				continue
		
			except Exception as e:
				logging.error(f"[CHECK_LIMIT_HANDLER] Error run: {e}")
	
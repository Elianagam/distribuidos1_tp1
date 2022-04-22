import logging

from common.constants import TIMEOUT_WAITING_MESSAGE
from metric_file_handler import MetricFileHandler
from queue import Empty
from queue import Queue
from threading import Thread


class ReportHandler(Thread):
	def __init__(self, queue_reports, stop_event):
		Thread.__init__(self)
		self._metrics_file = MetricFileHandler()
		self._queue_reports = queue_reports
		self._stop_event = stop_event

	def run(self):
		while not self._stop_event.is_set():
			try:
				metric = self._queue_reports.get(timeout=TIMEOUT_WAITING_MESSAGE)
				#logging.info(f"[REPORT_HANDLER] Metric get from queue: {metric}")
				self._queue_reports.task_done()
				self._metrics_file.write(metric)
				logging.info(f"[REPORT_HANDLER] Metric saved: {metric}")
			except Empty:
				if self._stop_event.is_set():
					return
				continue
			except Exception as e:
				logging.error(f"[QUERY_HANDLER] Error {e}")

		#self._queue_reports.join()
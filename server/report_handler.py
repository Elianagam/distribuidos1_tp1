from threading import Thread
import logging
from metric_file_handler import MetricFileHandler
from queue import Queue


class ReportHandler(Thread):
	def __init__(self, queue_reports, stop_event):
		Thread.__init__(self)
		self._metrics_file = MetricFileHandler()
		self._queue_reports = queue_reports
		self._stop_event = stop_event

	def close(self):
		self.is_alive = False

	def run(self):
		while not self._stop_event.is_set():
			try:
				metric = self._queue_reports.get(timeout=TIMEOUT_WAITING_MESSAGE)
				self._queue_reports.task_done()
				self._metrics_file.write(metric)
				logging.info(f"[REPORT HANDLER] Metric saved: {metric}")
			except:
				continue

		self._queue_reports.join()
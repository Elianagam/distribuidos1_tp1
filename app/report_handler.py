import logging

from common.constants import TIMEOUT_WAITING_MESSAGE
from metric_file_handler import MetricFileHandler
from queue import Empty
from multiprocessing import Process, Queue


class ReportHandler(Process):
	def __init__(self, queue_reports, stop_event):
		super(ReportHandler, self).__init__()
		self._metrics_file = MetricFileHandler()
		self._queue_reports = queue_reports
		self._stop_event = stop_event

	def run(self):
		while not self._stop_event.is_set():
			try:
				metric = self._queue_reports.get(timeout=TIMEOUT_WAITING_MESSAGE)
				#logging.info(f"[REPORT_HANDLER] Metric get from queue: {metric}")
				self._metrics_file.write(metric)
				logging.info(f"[REPORT_HANDLER] Metric saved: {metric}")
			except Empty:
				if self._stop_event.is_set():
					return
				continue
			except Exception as e:
				logging.error(f"[REPORT_HANDLER] Error {e}")


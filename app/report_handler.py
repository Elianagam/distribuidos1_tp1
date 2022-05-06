import logging

from common.constants import TIMEOUT_WAITING_MESSAGE
from metric_file_handler import MetricFileHandler
from queue import Empty
from multiprocessing import Process, Queue


class ReportHandler(Process):
	def __init__(self, queue_reports, stop_event, id, n_reporters):
		super(ReportHandler, self).__init__()
		self._metrics_file = MetricFileHandler()
		self._queue_reports = queue_reports
		self._stop_event = stop_event
		self._n_reporters = n_reporters
		self.id = id


	def can_write(self, metric):
		allowed_writer = int(metric["metric_id"]) % self._n_reporters
		return allowed_writer == self.id


	def run(self):
		while not self._stop_event.is_set():
			try:
				metric = self._queue_reports.get(timeout=TIMEOUT_WAITING_MESSAGE)

				if self.can_write(metric):
					self._metrics_file.write(metric)
					logging.info(f"[REPORT_HANDLER] Metric saved: {metric['metric_id']} by worker {self.id}")
				else:
					# if process is not allowed to write metric then put in queue again
					self._queue_reports.put(metric)
			except Empty:
				if self._stop_event.is_set():
					return
				continue
			except Exception as e:
				logging.error(f"[REPORT_HANDLER] Error {e}")


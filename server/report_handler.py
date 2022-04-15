from threading import Thread
import logging
from response import *
from metric_file_handler import MetricFileHandler


class ReportHandler(Thread):
	def __init__(self, queue_reports):
		Thread.__init__(self)
		#self._socket = socket
		self._metrics_file = MetricFileHandler()
		self._queue_reports = _queue_reports
		self._is_alive = True

"""
	def __save_metric(self, metric):
		self._metrics_file.write(metric)

	def __metric_is_valid(self, metric):
		return ("metric_id" in metric and type(metric["metric_id"]) is str) \
			and ("value" in metric and type(metric["value"]) is float)

	def __recv_metric(self):
		metric = self._socket.recv_message()
		logging.info(f"[METRIC_HANDLER] Recv Metric - {metric}")

		if (__metric_is_valid(metric)):
			msg = SuccessRecv().serialize()
			self._socket.send_message(msg)
			logging.info(f"[METRIC_HANDLER] {msg}")
		else:
			# TODO THROW ERROR OR NONE?
			msg = MetricBadRequest().serialize()
			self._socket.send_message(msg)
			logging.error(f"[METRIC_HANDLER] {msg}")

		return metric
"""

	def run(self):
		while self._is_alive:
			try:
				metric = self._queue_reports.get(timeout=TIMEOUT_WAITING_MESSAGE)
				self._queue_reports.task_done()
				self._metrics_file.write(metric)
				logging.info(f"[REPORT HANDLER] Metric saved: {metric}")
			except queue.Empty:
				continue

		self._queue_reports.join()
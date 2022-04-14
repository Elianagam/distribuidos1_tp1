from threading import Thread
import json
import logging
from response import *
from metric_file_handler import MetricFileHandler


class ServerWorker(Thread):

	def __init__(self, socket):
		Thread.__init__(self)
		self._socket = socket
		self._metrics_file = MetricFileHandler()


	def __save_metric(self, metric):
		self._metrics_file.write(metric)

	def __recv_metric(self):
		metric = self._socket.recv_message()
		logging.info(f"[SERVER WORKER] Recv Metric - {metric}")

		# TODO: Check in metricId is in file
		self._socket.send_message(SuccessRecv().serialize())
		return metric

	def __send_metrics(self, data):
		rows = self._metrics_file.read(data)
		self._socket.send_message(SuccessAggregation(rows).serialize())

		self._socket.send_message(CloseMessage().serialize())


	def run(self):
		recv = self._socket.recv_message()
		mode = recv["mode"]

		if mode == "report":
			self._socket.send_message(ValidMode().serialize())
			metric = self.__recv_metric()
			self.__save_metric(metric)

		elif mode == "agg":
			self._socket.send_message(ValidMode().serialize())

			self.__send_metrics(recv["data"])
		
		else:
			logging.error(f"[SERVER WORKER] Invalid mode: {mode}")
			self._socket.send_message(InvalidMode().serialize())

		self._socket.close_conection()


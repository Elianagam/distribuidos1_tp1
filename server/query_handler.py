from threading import Thread
import json
import logging
from response import *
from datetime import datetime
from metric_file_handler import MetricFileHandler
from common.vars import DATE_FORMAT


class QueryHandler(Thread):

	def __init__(self, socket, queue_querys):
		Thread.__init__(self)
		self._socket = socket
		self._metrics_file = MetricFileHandler()
		self._queue_querys = queue_querys


	def __metric_is_valid(self, metric):
		check_data =  ("metric_id" in metric and type(metric["metric_id"]) is str) \
			and ("aggregation" in metric and type(metric["aggregation"]) is str) \
			and ("aggregation_window_secs" in metric and type(metric["aggregation_window_secs"]) is float)

		try:
			# Para checkear si el formato fecha es correcto
			datetime.strptime(metric["from_date"], DATE_FORMAT)
			datetime.strptime(metric["to_date"], DATE_FORMAT)
			return check_data and True
		except e:
			return False



	def __recv_aggregation_query(self):
		query = self._socket.recv_message()
		logging.info(f"[METRIC_HANDLER] Recv Aggregation Query - {query}")

		is_valid = self.__metric_is_valid(metric)
		exists = self._metrics_file.exists()

		if exists and is_valid:
			msg = SuccessRecv().serialize()
			self._socket.send_message(msg)
			logging.info(f"[METRIC_HANDLER] {msg}")

		elif is_valid and not exists:
			msg = MetricIdNotFound().serialize()
			self._socket.send_message(msg)
			logging.error(f"[METRIC_HANDLER] {msg}")
			# TODO THROW ERROR OR NONE?

		elif not is_valid  and exist:
			msg = QueryBadRequest().serialize()
			self._socket.send_message(msg)
			logging.error(f"[QUERY HANDLER] Date Invalid Format. Data: {metric}, Valid format: {DATE_FORMAT}")
			# TODO THROW ERROR OR NONE?

		else:
			logging.error(f"[METRIC_HANDLER] Unkown Error")


		return query

	def __send_metrics(self, data):
		rows = self._metrics_file.read(data)
		self._socket.send_message(SuccessAggregation(rows).serialize())

		self._socket.send_message(CloseMessage().serialize())


	def run(self):
		query = self.__recv_aggregation_query()
		self.__send_metrics(query)

		self._socket.close_conection()



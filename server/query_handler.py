from threading import Thread
import json
import logging
from response import *
from datetime import datetime
from metric_file_handler import MetricFileHandler
from common.constants import DATE_FORMAT


class QueryHandler(Thread):

	def __init__(self, queue_querys):
		Thread.__init__(self)
		#self._socket = socket
		self._metrics_file = MetricFileHandler()
		self._queue_querys = queue_querys
		self._is_alive = True

"""
	def __recv_aggregation_query(self):
		query = self._socket.recv_message()
		logging.info(f"[METRIC_HANDLER] Recv Aggregation Query - {query}")

		is_valid = self.__query_is_valid(metric)
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
"""

	def __proccess_query(self, query):
		if (self._metrics_file.exists(query["metric_id"])):
			response = self._metrics_file.read(query)
		else:
			response = MetricIdNotFound().serialize()

		return response


	def __send_response(self, response, socket):
		socket.send_message(response)


	def run(self):
		while self._is_alive:
			try:
				request = self._queue_querys.get(timeout=TIMEOUT_WAITING_MESSAGE)
				self._queue_querys.task_done()
				logging.info(f"[QUERY_HANDLER] Recv Aggregation Query - {request}")

				response = self._metrics_file.read(request["query"])
				self.__send_response(response, request["socket"])

				logging.info(f"[QUERY_HANDLER] Query proccessed: {request}, Agg result: {agg_result}")
			except self._queue_querys.Empty:
				continue

		self._queue_querys.join()





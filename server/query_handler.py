from threading import Thread
import json
import logging
from response import *
from metric_file_handler import MetricFileHandler
from queue import Queue
from common.constants import DATE_FORMAT, TIMEOUT_WAITING_MESSAGE


class QueryHandler(Thread):

	def __init__(self, queue_querys):
		Thread.__init__(self)
		self._metrics_file = MetricFileHandler()
		self._queue_querys = queue_querys
		self._is_alive = True

	def __proccess_query(self, query):
		if (self._metrics_file.exists(query["metric_id"])):
			logging.info(f"[QUERY_HANDLER] Read metric file - {request}")
			agg_result = self._metrics_file.read(query)

			response = SuccessAggregation(agg_result).serialize()
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
			except:
				continue

		self._queue_querys.join()





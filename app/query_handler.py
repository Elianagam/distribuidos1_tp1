import logging

from common.constants import TIMEOUT_WAITING_MESSAGE
from messages.response import MetricIdNotFound
from messages.response import SuccessAggregation
from metric_file_handler import MetricFileHandler
from queue import Empty
from queue import Queue
from threading import Thread


class QueryHandler(Thread):

	def __init__(self, queue_querys, queue_reponses, stop_event):
		Thread.__init__(self)
		self._metrics_file = MetricFileHandler()
		self._queue_querys = queue_querys
		self._queue_reponses = queue_reponses
		self._stop_event = stop_event


	def __proccess_query(self, query, recv_socket):
		try:
			agg_result = self._metrics_file.aggregate(query)
			response = SuccessAggregation(agg_result).serialize()

			#logging.info(f"[QUERY_HANDLER] Query proccessed, Agg result: {response}")
			self._queue_reponses.put({"response":response, "socket": recv_socket})

		except Exception as e:
			logging.error(f"[QUERY_HANDLER] Send response fail {e}")	


	def run(self):
		while not self._stop_event.is_set():
			try:
				request = self._queue_querys.get(timeout=TIMEOUT_WAITING_MESSAGE)
				logging.info(f"[QUERY_HANDLER] Recv Aggregation Query - {request['query']}")

				self.__proccess_query(request["query"], request["socket"])
				self._queue_querys.task_done()

			except Empty:
				if self._stop_event.is_set():
					return
				continue
			except Exception as e:
				logging.error(f"[QUERY_HANDLER] Error {e}")



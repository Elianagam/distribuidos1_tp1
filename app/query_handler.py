import logging

from common.constants import TIMEOUT_WAITING_MESSAGE
from messages.response import MetricIdNotFound
from messages.response import SuccessAggregation
from metric_file_handler import MetricFileHandler
from queue import Empty
from multiprocessing import Process, Queue



class QueryHandler(Process):

	def __init__(self, queue_querys, queue_reponses, stop_event):
		super(QueryHandler, self).__init__()
		self._metrics_file = MetricFileHandler()
		self._queue_querys = queue_querys
		self._queue_reponses = queue_reponses
		self._stop_event = stop_event


	def __proccess_query(self, query, recv_socket):
		try:
			agg_result = self._metrics_file.aggregate(query)
			response = SuccessAggregation(agg_result).serialize()

			#logging.info(f"[QUERY_HANDLER] Query proccessed, Agg result: {response}")
			if not self._queue_reponses.full():
				self._queue_reponses.put({"response":response, "socket": recv_socket})
			else:
				raise RuntimeError(f"Response Queue is Full")
		except Exception as e:
			logging.error(f"[QUERY_HANDLER] Send response fail {e}")	


	def run(self):
		while not self._stop_event.is_set():
			try:
				request = self._queue_querys.get(timeout=TIMEOUT_WAITING_MESSAGE)
				logging.info(f"[QUERY_HANDLER] Recv Aggregation Query - {request['query']}")
				self.__proccess_query(request["query"], request["socket"])

			except Empty:
				if self._stop_event.is_set():
					return
				continue
			except Exception as e:
				logging.error(f"[QUERY_HANDLER] Error {e}")



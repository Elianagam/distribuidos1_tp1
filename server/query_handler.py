from threading import Thread
import logging
from response import SuccessAggregation, MetricIdNotFound
from metric_file_handler import MetricFileHandler
from queue import Queue, Empty
from common.constants import TIMEOUT_WAITING_MESSAGE


class QueryHandler(Thread):

	def __init__(self, queue_querys, stop_event):
		Thread.__init__(self)
		self._metrics_file = MetricFileHandler()
		self._queue_querys = queue_querys
		self._stop_event = stop_event


	def __proccess_query(self, query, recv_socket):
		try:
			if (self._metrics_file.exists(query["metric_id"])):
				agg_result = self._metrics_file.aggregate(query)
				response = SuccessAggregation(agg_result).serialize()
			else:
				response = MetricIdNotFound().serialize()

			logging.info(f"[QUERY_HANDLER] Query proccessed, Agg result: {response}. Send to: {recv_socket.get_addr()}")
			recv_socket.send_message(response)

		except Exception as e:
			logging.error(f"[QUERY_HANDLER] Send response fail {e}")
		
		finally:
			logging.info(f"[QUERY_HANDLER] CLOSE SOCKET")
			recv_socket.close_connection()


	def run(self):
		while not self._stop_event.is_set():
			try:
				request = self._queue_querys.get(timeout=TIMEOUT_WAITING_MESSAGE)
				logging.info(f"[QUERY_HANDLER] Recv Aggregation Query - {request['query']}")

				self.__proccess_query(request["query"], request["socket"])
				self._queue_querys.task_done()

			except Empty:
				continue
			except Exception as e:
				logging.error(f"[QUERY_HANDLER] Error {e}")



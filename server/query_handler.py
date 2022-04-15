from threading import Thread
import json
import logging
from response import *
from metric_file_handler import MetricFileHandler
from queue import Queue, Empty
from common.constants import TIMEOUT_WAITING_MESSAGE
from common.socket import Socket


class QueryHandler(Thread):

	def __init__(self, queue_querys):
		Thread.__init__(self)
		self._metrics_file = MetricFileHandler()
		self._queue_querys = queue_querys
		self._is_alive = True

	def __proccess_query(self, query, recv_socket):
		try:
			if (self._metrics_file.exists(query["metric_id"])):
				agg_result = self._metrics_file.read(query)
				response = SuccessAggregation(agg_result).serialize()
			else:
				response = MetricIdNotFound().serialize()

			logging.info(f"[QUERY_HANDLER] Recv Aggregation SOCKET - {recv_socket}")

			new_connection = Socket(recv_socket["host"], recv_socket["port"])
			new_connection.connect()

			new_connection.send_message(response)
			logging.debug(f"[QUERY_HANDLER] Send response SOCKET: ({new_connection._host},{new_connection._port})")
			logging.info(f"[QUERY_HANDLER] Query proccessed, Agg result: {response}")

		except Exception as e:
			logging.error(f"[QUERY_HANDLER] Send response fail {e}")
		
		finally:
			new_connection.close_conection()


	def run(self):
		while self._is_alive:
			try:
				request = self._queue_querys.get(timeout=TIMEOUT_WAITING_MESSAGE)
				logging.info(f"[QUERY_HANDLER] Recv Aggregation Query - {request['query']}")

				self.__proccess_query(request["query"], request["socket"])
				self._queue_querys.task_done()

			except Empty:
				continue
			except Exception as e:
				logging.error(f"[QUERY_HANDLER] Error {e}")

		self._queue_querys.join()


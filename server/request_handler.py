from threading import Thread
import logging
from queue import Queue, Empty
from report_handler import ReportHandler
from query_handler import QueryHandler
from common.socket import Socket
from common.constants import *
from messages.request import *
from messages.response import *


class RequestHandler(Thread):
	def __init__(self, port, listen_backlog, queue_size, stop_event, n_workers):
		Thread.__init__(self)
		self._socket = Socket('', port)
		self._socket.bind_and_listen(listen_backlog)
		self._stop_event = stop_event

		self._queue_clients = Queue()
		self._client_handlers = [Thread(target=self.__handle_client_connection) for i in range(n_workers)]

		self._queue_reports = Queue(maxsize=queue_size)
		self._report_handler = ReportHandler(self._queue_reports, self._stop_event)

		self._queue_querys = Queue(maxsize=queue_size)
		self._query_handlers = [QueryHandler(self._queue_querys, self._stop_event) for i in range(n_workers)]

	def __start_threads(self):
		self._report_handler.start()

		for w in self._query_handlers:
			w.start()

		for client in self._client_handlers:
			client.start()

	def run(self):
		self.__start_threads()

		while not self._stop_event.is_set():
			try:
				client_socket = self._socket.accept_new_connection()
				self._queue_clients.put(client_socket)

			except OSError as e:
				logging.info(f"[REQUEST_HANDLER] Error operating with socket: {e}")

		self.__close_all()


	def __handle_client_connection(self):
		while not self._stop_event.is_set():
			try:
				client_socket = self._queue_clients.get(timeout=TIMEOUT_WAITING_MESSAGE)
				self.__handle_client_request(client_socket)
			
			except Empty:
				continue

			except OSError as e:
				logging.info(f"[REQUEST_HANDLER] Error operating with socket: {e}")


	def __handle_client_request(self, client_socket):
		recv = client_socket.recv_message()
		mode = recv["mode"]
		logging.debug(f"[REQUEST_HANDLER] Recv mode: {mode}")

		if mode == MODE_REPORT:
			logging.debug(f"[REQUEST_HANDLER] Data: {recv['data']}")
			status_code = self.__add_report(recv["data"])
			self.__send_client_response(client_socket, status_code, recv["data"])
			client_socket.close_connection()

		elif mode == MODE_AGG:
			logging.debug(f"[REQUEST_HANDLER] Data: {recv['data']}")
			status_code = self.__add_query(client_socket, recv["data"])
			self.__send_client_response(client_socket, status_code, recv["data"])
	
		else:
			logging.error(f"[REQUEST HANDLER] Invalid mode: {mode}")
			client_socket.send_message(InvalidMode().serialize())
			client_socket.close_connection()


	def __add_report(self, metric):
		if not self._queue_reports.full():
			if ReportMetric(metric).is_valid():
				#logging.debug(f"[REQUEST_HANDLER] Add new metric to queue ")
				self._queue_reports.put(metric)
				return SUCCESS_STATUS_CODE
			else:
				return CLIENT_METRIC_ERROR
		return SERVER_ERROR


	def __add_query(self, client_socket, query):
		if not self._queue_querys.full():
			if AggregationQuery(query).is_valid():
				#logging.debug(f"[REQUEST_HANDLER] Add new query to queue")
				self._queue_querys.put({"query": query, "socket": client_socket})
				return SUCCESS_STATUS_CODE
			else:
				return CLIENT_AGG_ERROR
		return SERVER_ERROR


	def __send_client_response(self, client_socket, status_code, data):
		if status_code == SUCCESS_STATUS_CODE:
			msg = SuccessRecv()

		elif status_code == CLIENT_METRIC_ERROR:
			msg = MetricBadRequest()

		elif status_code == CLIENT_AGG_ERROR:
			msg = QueryBadRequest()

		elif status_code == SERVER_ERROR:
			msg = ServerError()

		msg = msg.serialize()
		client_socket.send_message(msg)
		logging.info(f"[REQUEST_HANDLER] Send response: {msg}. To: {client_socket.get_addr()}")


	def __close_all(self):
		logging.info("[REQUEST_HANDLER] Close all connections")
		self._queue_clients.join()

		for c in self._client_handlers:
			c.join()

		self._queue_reports.join()
		self._report_handler.join()

		self._queue_querys.join()
		self._query_handler.join()

		self._socket.close_connection()


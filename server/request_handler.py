import socket
from threading import Thread, Timer
import traceback
import os
import json
import logging
from queue import Queue
from request_handler import ReportHandler
from query_handler import QueryHandler
from common.socket import Socket
from common.constants import MODE_REPORT, MODE_AGG, SUCCESS, CLIENT_ERROR, SERVER_ERROR
from request import *
from response import *


class RequestHandler(Thread):
	def __init__(self, port, listen_backlog, queue_size):
		Thread.__init__(self)
		self._socket = Socket('', port)
		self._socket.bind_and_listen(listen_backlog)

		self._is_alive = True
		#self._clients = []
		self._queue_reports = Queue(maxsize=queue_size)
		self._queue_querys = Queue(maxsize=queue_size)
		self._request_handler = ReportHandler(self._queue_reports)
		self._query_handler = QueryHandler(self._queue_querys)
		self._request_handler.start()
		self._query_handler.start()


	def run(self):
		while self._is_alive:
			new_socket = None
			try:
				new_socket = self._socket.accept_new_connection()
				self.__read_mode(new_socket)

			except OSError as e:
				logging.info(f"[HANDLER] Error operating with socket: {e}")

			finally:
				if new_socket != None:
					new_socket.close()
			

		#self._join_all()

	def __read_mode(self, new_socket):
		try:
			recv = new_socket.recv_message()
			mode = recv["mode"]

			if mode == MODE_REPORT:
				is_done = self.__add_report(recv["data"])
				self.__send_client_response(new_socket, is_done, recv["data"])

			elif mode == MODE_AGG:
				is_done = self.__add_data(new_socket, recv["data"])
				self.__send_client_response(new_socket, is_done)
		
			else:
				logging.error(f"[REQUEST HANDLER] Invalid mode: {mode}")
				new_socket.send_message(InvalidMode().serialize())


	def __add_report(self, metric):
		if not self._queue_querys.full():
			if ReportMetric(metric).is_valid():
				self._queue_querys.put(metric)
				return SUCCESS
			else:
				return CLIENT_METRIC_ERROR
		return SERVER_ERROR


	def __add_query(self, new_socket, query):
		if not self._queue_reports.full():
			if AggregationQuery(query).is_valid():
				self._queue_reports.put({"query": query, "socket": new_socket})
				return SUCCESS
			else:
				return CLIENT_AGG_ERROR
		return SERVER_ERROR

	def __send_client_response(self, new_socket, status_code, data):
		if status_code == SUCCESS:
			msg = SuccessRecv()

		elif status_code == CLIENT_METRIC_ERROR:
			msg = MetricBadRequest()

		elif status_code == CLIENT_AGG_ERROR:
			msg = QueryBadRequest()

		elif status_code == SERVER_ERROR:
			msg = ServerError()

		msg = msg.serialize()
		new_socket.send_message(msg)
		logging.info(f"[REQUEST_HANDLER] {msg}. Data {data}")


	def _join_all(self):
		logging.info("[REQUEST_HANDLER] Close all connections")
		for client in self._clients:
			client.join()

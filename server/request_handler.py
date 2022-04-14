import socket
from threading import Thread, Timer
import traceback
import os
import json
import logging
from queue import Queue
from server_worker import ServerWorker
from common.socket import Socket


class RequestHandler(Thread):
	def __init__(self, port, listen_backlog):
		Thread.__init__(self)
		self._socket = Socket('', port)
		self._socket.bind_and_listen(listen_backlog)
		
		self._clients = []
		self._new_metrics = Queue()
		self._agg_consults = Queue()


	def run(self):
		while True:
			new_socket = self._socket.accept_new_connection()
			
			new_client = ServerWorker(new_socket)
			new_client.start()
			self._clients.append(new_client)

		self._join_all()
		

	def _join_all(self):
		logging.info("[REQUEST_HANDLER] Close all connections")
		for client in self._clients:
			client.join()

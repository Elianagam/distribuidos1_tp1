import socket
import logging
import json
from common.utils import *

class Socket():
	def __init__(self, host, port, conn=None):
		if conn == None:
			self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		else:
			self._socket = conn
		self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self._host = host
		self._port = port

	def get_addr(self):
		return {"port": self._port, "host": self._host}

	def close_connection(self):
		#self._socket.shutdown(socket.SHUT_RDWR)
		self._socket.close()
		#logging.info(f"[SOCKET] Close from ({self._host}, {self._port})")


	def send_message(self, message):
		try:
			len_msg = int_to_bytes(len(message))
			self._socket.sendall(len_msg)
			self._socket.sendall(message.encode())
		except:
			raise RuntimeError("[SOCKET] Connection failed while send")


	def recv_message(self, buffer_size=1024):
		try:
			# First recv len in byts
			int_bytes = self.__recvall(4)
			data_size = int_from_bytes(int_bytes)
			recv = self.__recvall(data_size)
			msg = json.loads(recv.decode())
			return msg
		except OSError:
			logging.info("[SOCKET] Error while reading socket {}".format(client_sock))
		return None


	def bind_and_listen(self, listen_backlog):
		self._socket.bind((self._host, self._port))
		self._socket.listen(listen_backlog)


	def accept_new_connection(self):
		"""
		Accept new connections
		Function blocks until a connection to a client is made.
		Then connection created is printed and returned
		"""

		# Connection arrived
		#logging.info("[SOCKET] Proceed to accept new connections")
		c, (client_host, client_port) = self._socket.accept()
		#logging.info(f"[SOCKET] Got connection from ({client_host}, {client_port})")
		new_socket = Socket(client_host, client_port, c)
		return new_socket


	def connect(self):
		self._socket.connect((self._host, self._port))


	def __recvall(self, data_size):
		recvd = bytearray()
		bytes_recvd = 0
		while bytes_recvd < data_size:
			b_recv = self._socket.recv(data_size - bytes_recvd)
			if not b_recv:
				msg = "[SOCKET] Connection failed while recv"
				logging.error(msg)
				raise RuntimeError(msg)
			bytes_recvd += len(b_recv)
			recvd.extend(b_recv)
		return recvd

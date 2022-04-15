import socket
import logging
import json

class Socket():
	def __init__(self, host, port, conn=None):
		if conn == None:
			self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		else:
			self._socket = conn
		self._host = host
		self._port = port


	def close_conection(self):
		#self._socket.shutdown(socket.SHUT_RDWR)
		self._socket.close()
		logging.info(f"[SOCKET] Close from ({self._host}, {self._port})")


	def send_message(self, response):
		self._socket.send(response.encode())

	def recv_message(self, buffer_size=1024):
		#try
		recv = self._socket.recv(buffer_size)
		msg = json.loads(recv.decode())
		return msg
		#except OSError:
		#    logging.info("Error while reading socket {}".format(client_sock))
		#return None


	def bind_and_listen(self, listen_backlog):
		self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self._socket.bind(('', self._port))
		self._socket.listen(listen_backlog)


	def accept_new_connection(self):
		"""
		Accept new connections
		Function blocks until a connection to a client is made.
		Then connection created is printed and returned
		"""

		# Connection arrived
		logging.info("[SOCKET] Proceed to accept new connections")
		c, (client_port, client_host) = self._socket.accept()
		logging.info(f"[SOCKET] Got connection from ({client_host}, {client_port})")
		new_socket = Socket(client_host, client_port, c)
		return new_socket


	def connect(self):
		self._socket.connect((self._host, self._port))



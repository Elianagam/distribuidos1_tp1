import socket
import json
import logging
from common.socket import Socket


class Client():
    def __init__(self, host, port, mode):
        self._socket = Socket(host, port)
        self._socket.connect()
        self._mode = mode

#    def send_message(self, message):
#        self.socket.send(message.encode())#

#    def recv_message(self):
#        recv = self.socket.recv(self.buffer_size)
#        response = json.loads(recv.decode())
#        return response


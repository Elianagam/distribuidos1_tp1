import socket
import json
import logging


class Client():
    def __init__(self, host, port, mode, buffer_size=1024):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        self.mode = mode
        self.buffer_size = buffer_size

    def send_message(self, message):
        self.socket.send(message.encode())

    def recv_message(self):
        recv = self.socket.recv(self.buffer_size)
        response = json.loads(recv.decode())
        return response


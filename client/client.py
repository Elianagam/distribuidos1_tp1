import socket
import json
import os
import logging
from messages.report_metric_message import ReportMetricMessage


class Client():
    def __init__(self, host, port, mode):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        self.mode = mode


    def send_message(self, message):
        self.socket.send(message.encode())

    def recv_message(self):
        rcv_packet = self.socket.recv(1024)
        response = json.loads(rcv_packet.decode())
        return response


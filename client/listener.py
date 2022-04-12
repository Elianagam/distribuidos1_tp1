import socket
import json
import os
import logging
from messages.report_metric_message import ReportMetricMessage
from client import Client


class Listener(Client):
    def __init__(self, host, port):
        super().__init__(host, port, "listen")

    def __send_message(self, message):
        super().send_message(message)

    def __recv_message(self):
        return super().recv_message()


    def run(self):
        self.__send_message(self.mode)
        response = self.__recv_message()
        logging.info(response)

        if (response['status'] == 200):
            while (response['msg'] != 'close'):
                recv = self.socket.recv(4096)
                response = json.loads(recv.decode())
                logging.info(response)

        else:
            logging.info(response)
        
        logging.info("Close socket")
        self.socket.close()
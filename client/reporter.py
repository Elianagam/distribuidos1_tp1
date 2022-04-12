import socket
import json
import os
import logging
from messages.report_metric_message import ReportMetricMessage
from client import Client

class Reporter(Client):
    def __init__(self, host, port):
        super().__init__(host, port, "report")

    def __send_message(self, message):
        super().send_message(message)

    def __recv_message(self):
        return super().recv_message()
    
    def __send_report(self, metric_id, value):
        metric_msg = ReportMetricMessage(metric_id, value).serialize()
        self.__send_message(metric_msg)

        metric_response = self.__recv_message()
        if (metric_response['status'] == "200"):
            logging.info(metric_response)

        elif (metric_response['status'] != "200"):
            logging.info(metric_response)


    def run(self, metric_id, value):
        self.__send_message(self.mode)
        mode_response = self.__recv_message()
        logging.info(mode_response)

        if (mode_response['status'] == 200):
            self.__send_report(metric_id, value)

        else:
            logging.info(mode_response)
        
        logging.info("Close socket")
        self.socket.close()

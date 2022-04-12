import socket
import json
import os
import logging
from messages.report_metric_message import ReportMetricMessage
from client import Client


class Listener(Client):
    def __init__(self, host, port):
        super().__init__(host, port, "listener")


    def run(self):
        pass
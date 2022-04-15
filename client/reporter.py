import json
import logging
from client import Client
from datetime import datetime
from common.constants import SUCCESS_STATUS_CODE, MODE_REPORT

class Reporter(Client):
    def __init__(self, host, port):
        super().__init__(host, port, MODE_REPORT)


    def run(self, metric_id, value):
        self._socket.send_message(json.dumps({"mode": self._mode,
            "data": ReportMetricMessage(metric_id, value).__dict__
            }))
        logging.info("send metric")
        response = self._socket.recv_message()
        logging.info(f"[REPORTER] {response}")
        
        
        self._socket.close_conection()


class ReportMetricMessage:
    def __init__(self, metric_id, value):
        self.metric_id = metric_id
        self.value = value
        self.datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def serialize(self):
        return json.dumps(self.__dict__)
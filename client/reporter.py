import json
import logging
from client import Client
from datetime import datetime

class Reporter(Client):
    def __init__(self, host, port):
        super().__init__(host, port, "report")
    
    def __send_report(self, metric_id, value):
        self._socket.send_message(ReportMetricMessage(metric_id, value).serialize())

        metric_response = self._socket.recv_message()
        if (metric_response['status'] == "200"):
            logging.info(metric_response)

        elif (metric_response['status'] != "200"):
            logging.info(metric_response)


    def run(self, metric_id, value):
        self._socket.send_message(json.dumps({"mode": self._mode}))
        mode_response = self._socket.recv_message()
        logging.info(mode_response)

        if (mode_response['status'] == 200):
            self.__send_report(metric_id, value)

        else:
            logging.info(mode_response)
        
        self._socket.close_conection()


class ReportMetricMessage:
    def __init__(self, metric_id, value):
        self.metric_id = metric_id
        self.value = value
        self.datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def serialize(self):
        return json.dumps(self.__dict__)
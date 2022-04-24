import json
import logging
import pytz

from datetime import datetime, timezone
from common.constants import SUCCESS_STATUS_CODE, MODE_REPORT
from common.socket import Socket

class ReporterClient:
    def __init__(self, host, port):
        self._socket = Socket(host, port)
        self._socket.connect()
        self._mode = MODE_REPORT

    def run(self, metric):
        self._socket.send_message(json.dumps({"mode": self._mode,
            "data": ReportMetricMessage(metric["metric_id"], metric["value"]).__dict__
            }))
        logging.info(f"[REPORTER] Metric to send: {metric}")
        response = self._socket.recv_message()
        logging.info(f"[REPORTER] {response}")
        
        
        self._socket.close_connection()


class ReportMetricMessage:
    def __init__(self, metric_id, value):
        self.metric_id = metric_id
        self.value = value
        tz = pytz.timezone('America/Argentina/Buenos_Aires')

        self.datetime = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    def serialize(self):
        return json.dumps(self.__dict__)
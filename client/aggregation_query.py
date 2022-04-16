import json
import logging
from common.constants import SUCCESS_STATUS_CODE, MODE_AGG
from common.socket import Socket

class AggregationQuery:
    def __init__(self, host, port):
        self._socket = Socket(host, port)
        self._socket.connect()
        self._mode = MODE_AGG

    def run(self, query):
        logging.info(f"[CLIENT] Send query {query}")
        self._socket.send_message(json.dumps({"mode": self._mode, "data": query}))
        logging.info("[CLIENT] Wait response for mode ")

        response = self._socket.recv_message()

        if (response['status'] == SUCCESS_STATUS_CODE):
            # Espera el resultado de la agregacion
            logging.info("[CLIENT] Wait agg respose...")
            response = self._socket.recv_message()
            logging.info(response)

        if (response['status'] != SUCCESS_STATUS_CODE):
            logging.error(response)
        
        self._socket.close_connection()

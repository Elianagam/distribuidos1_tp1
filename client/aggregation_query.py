import json
import logging
from client import Client
from common.constants import SUCCESS_STATUS_CODE, MODE_AGG


class AggregationQuery(Client):
    def __init__(self, host, port):
        super().__init__(host, port, MODE_AGG)


    def run(self, query):
        self._socket.send_message(json.dumps({"mode": self._mode, "data": query}))
        response = self._socket.recv_message()

        if (response['status'] == SUCCESS_STATUS_CODE):
            response = self._socket.recv_message(4096)
            logging.info(response)

        if (response['status'] != SUCCESS_STATUS_CODE):
            logging.error(response)
        
        self._socket.close_conection()

import json
import logging
from client import Client


class AggregationQuery(Client):
    def __init__(self, host, port):
        super().__init__(host, port, "agg")

    def run(self):
        self._socket.send_message(json.dumps({"mode": self._mode,
            "data": {"metric_id": 2,
                    "from_date":"2022-04-11 00:00:00",
                    "to_date":"2022-04-14 00:00:00",
                    "aggregation":"SUM",
                    "aggregation_window_secs":0
                    }
            }))
        response = self._socket.recv_message()
        logging.info(response)

        if (response['status'] == 200):
            while (response['msg'] != 'close'):
                response = self._socket.recv_message(4096)
                logging.info(response)

        else:
            logging.info(response)
        
        self._socket.close_conection()

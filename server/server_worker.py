from threading import Thread
import json
import logging
from response import *

class ServerWorker(Thread):

    def __init__(self, port, host, socket):
        Thread.__init__(self)
        self.port = port
        self.host = host
        self.socket = socket
        self.buffer_size = 1024

    def __close_conection(self):
        self.socket.close()

    def __send_response(self, response):
        self.socket.send(response.serialize().encode())

    def __recv_request(self):
#        try:
#            msg = client_sock.recv(1024).rstrip().decode('utf-8')
        recv = self.socket.recv(self.buffer_size)
        response = json.loads(recv.decode())
#        except OSError:
#            logging.info("Error while reading socket {}".format(client_sock))
#        finally:
#            client_sock.close()

        return response

    def __recv_mode(self):
        mode = self.socket.recv(self.buffer_size).decode()
        logging.info(f"Mode: {mode}")
        return mode


    def run(self):
        mode = self.__recv_mode()

        if mode == "report":
            recv_msg = self.recv_metric()
        
        else:
            logging.error(f"Invalid mode: {mode}")
            self.__send_response(InvalidMode())

        self.__close_conection()


    def recv_metric(self):
        self.__send_response(ValidMode())
        
        metric = self.__recv_request()

        # TODO: Check in metricId is in file
        self.__send_response(SuccessRecv())
        logging.info(f"Recv Metric - {metric}")

        # TODO: self.save_metric()
from threading import Thread
import json
import logging
from response import *

class ServerWorker(Thread):

    def __init__(self, port, host, socket, file_writer):
        Thread.__init__(self)
        #self.port = port
        #self.host = host
        self._socket = socket
        self._file_writer = file_writer

    def __close_conection(self):
        #self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()

    def __send_response(self, response):
        self._socket.send(response.serialize().encode())

    def __recv_request(self):
#        try:
#            msg = client_sock.recv(1024).rstrip().decode('utf-8')
        recv = self._socket.recv(1024)
        response = json.loads(recv.decode())
#        except OSError:
#            logging.info("Error while reading socket {}".format(client_sock))
#        finally:
#            client_sock.close()
        return response

    def __recv_mode(self):
        mode = self._socket.recv(1024).decode()
        logging.info(f"Mode: {mode}")
        return mode

    def __save_metric(self, metric):
        self._file_writer.write(metric)

    def __recv_metric(self):
        self.__send_response(ValidMode())
        
        metric = self.__recv_request()

        # TODO: Check in metricId is in file
        self.__send_response(SuccessRecv())
        logging.info(f"Recv Metric - {metric}")

        self.__save_metric(metric)

    def run(self):
        mode = self.__recv_mode()

        if mode == "report":
            self.__recv_metric()
        
        else:
            logging.error(f"Invalid mode: {mode}")
            self.__send_response(InvalidMode())

        self.__close_conection()


    

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
        self._metrics_file = file_writer

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
        self._metrics_file.write(metric)

    def __recv_metric(self):
        metric = self.__recv_request()
        logging.info(f"Recv Metric - {metric}")

        # TODO: Check in metricId is in file
        self.__send_response(SuccessRecv())
        return metric

    def __send_metrics(self):
        rows = self._metrics_file.read()
        self.__send_response(SuccessAggregation(rows))

        self.__send_response(CloseListener())
        print("-- send close msg")


    def run(self):
        mode = self.__recv_mode()

        if mode == "report":
            self.__send_response(ValidMode())
            metric = self.__recv_metric()
            self.__save_metric(metric)

        if mode == "listen":
            self.__send_response(ValidMode())
            self.__send_metrics()
        
        else:
            logging.error(f"Invalid mode: {mode}")
            self.__send_response(InvalidMode())

        self.__close_conection()


    

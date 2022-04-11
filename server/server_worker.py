from threading import Thread
import json
from response import *

class ServerWorker(Thread):
    def __init__(self, port, host, socket, source_dir):
        Thread.__init__(self)
        self.port = port
        self.host = host
        self.socket = socket
        self.source_dir = source_dir
        self.buffer_size = 1024

    def __close_conection(self):
        self.socket.close()

    def __send_response(self, response):
        self.socket.send(response.serialize().encode())

    def __recv_request(self):
        self.socket.recv(self.buffer_size).decode()

    def run(self):
        mode = __recv_request()

        if mode == "report":
            print(f"Client {self.host}:{self.port} mode: {mode} - recving")
            self.__send_response(ValidMode())
            metric = __recv_request()
            # save metric
            print(metric)
        
        else:
            print("Invalid mode: ", mode)
            self.__send_response(InvalidMode())


    def post_metric(self):
        return "metric"

    def get_metric(self):
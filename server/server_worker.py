from threading import Thread
import json
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
        recv = self.socket.recv(self.buffer_size)
        response = json.loads(recv.decode())
        return response

    def __recv_mode(self):
        return self.socket.recv(self.buffer_size).decode()

    def run(self):
        mode = self.__recv_mode()
        print(mode)

        if mode == "report":
            print(f"Client {self.host}:{self.port} mode: {mode} - recving")
            self.__send_response(ValidMode())
            metric = self.__recv_request()
            self.__send_response(SuccessRecv())
            # save metric
            print(metric)
        
        else:
            print("Invalid mode: ", mode)
            self.__send_response(InvalidMode())


    def save_metric(self):
        pass

    def post_metric(self):
        pass
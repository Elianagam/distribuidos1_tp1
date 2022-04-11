import socket
import json
import os
from messages.report_metric_message import ReportMetricMessage

class Client():
    def __init__(self, host, port=6000):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.buffer_size = 1024

    def __connect(self):
        print("Success connection")
        self.socket.connect((self.host, self.port))

    def __send_message(self, message):
        self.socket.send(message.encode())

    def __recv_message(self):
        rcv_packet = self.socket.recv(self.buffer_size)
        response = json.loads(rcv_packet.decode())
        return response


class ConsumerAggregation(Client):
    def __init__(self, host, port):
        super().__init__(host, port)


    def run(self):
        #super().__connect()
        pass


class Reporter(Client):
    def __init__(self, host, port):
        self.mode = "report"
        super().__init__(host, port)

    def __connect(self):
        print("Success connection")
        self.socket.connect((self.host, self.port))

    def __send_message(self, message):
        self.socket.send(message.encode())

    def __recv_message(self):
        recv = self.socket.recv(self.buffer_size)
        response = json.loads(recv.decode())
        return response
    
    def __send_report(self, metric_id, value):
        # Create metric and send
        metric_msg = ReportMetricMessage(metric_id, value).serialize()
        self.__send_message(metric_msg)

        metric_response = self.__recv_message()
        if (metric_response['status'] == "200"):
            # TODO log success
            print(metric_response)
            

        elif (metric_response['status'] != "200"):
            # TODO log error
            print(metric_response)


    def run(self, metric_id, value):
        self.__connect()
        # TODO: eliminar print
        print(self.mode)

        self.__send_message(self.mode)
        mode_response = self.__recv_message()

        if (mode_response['status'] == 200):
            self.__send_report(metric_id, value)

        else:
            # TODO log error
            print(mode_response)

        
        # TODO log close
        print("Close socket")
        self.socket.close()

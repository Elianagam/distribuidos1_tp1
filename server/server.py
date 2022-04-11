import socket
from threading import Thread, Timer
import traceback
import os
import json
from server_worker import ServerWorker


class Server(Thread):
    def __init__(self, port, host):
        Thread.__init__(self)
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.host = host
        self.clients = []

    def __setup__(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen()
        print(f"Listening in {self.host} and {self.port}")

    def main_loop(self):
        self.__setup__()
        while True:
            conn, (client_host, client_port) = self.socket.accept()
            
            print(f'Incoming Connection from {client_host} {client_port}')
            new_client = ServerWorker(client_port, client_host, conn)
            new_client.start()
            self.clients.append(new_client)

        print("Cerrando conexiones")
        for client in self.clients:
            client.join()


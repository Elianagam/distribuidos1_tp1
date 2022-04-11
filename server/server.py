import socket
from threading import Thread, Timer
import traceback
import os
import json
import logging
from server_worker import ServerWorker


class Server(Thread):
    def __init__(self, port, listen_backlog):
        Thread.__init__(self)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', port))
        self.socket.listen(listen_backlog)
        self.clients = []

    def __accept_new_connection(self):
        """
        Accept new connections
        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info("Proceed to accept new connections")
        c, (client_port, client_host) = self.socket.accept()
        logging.info('Got connection from ({}, {})'.format(client_host, client_port))
        return c, client_port, client_host


    def run(self):
        while True:
            conn, client_port, client_host = self.__accept_new_connection()
            
            new_client = ServerWorker(client_port, client_host, conn)
            new_client.start()
            self.clients.append(new_client)

        print("Cerrando conexiones")
        for client in self.clients:
            client.join()


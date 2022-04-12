import socket
from threading import Thread, Timer
import traceback
import os
import json
import logging
from server_worker import ServerWorker
from safe_file_writer import SafeFileWriter


class Server(Thread):
    def __init__(self, port, listen_backlog, filename):
        Thread.__init__(self)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(('', port))
        self._socket.listen(listen_backlog)
        
        self._clients = []
        self._file_writer = SafeFileWriter(filename)

    def __accept_new_connection(self):
        """
        Accept new connections
        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info("Proceed to accept new connections")
        c, (client_port, client_host) = self._socket.accept()
        logging.info('Got connection from ({}, {})'.format(client_host, client_port))
        return c, client_port, client_host


    def run(self):
        while True:
            conn, client_port, client_host = self.__accept_new_connection()
            
            new_client = ServerWorker(client_port, client_host, conn, self._file_writer)
            new_client.start()
            self._clients.append(new_client)

        print("Cerrando conexiones")
        for client in self._clients:
            client.join()


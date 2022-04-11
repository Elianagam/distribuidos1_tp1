import argparse
import socket
from server import Server

parser = argparse.ArgumentParser()

parser.add_argument('-p', '--port', help="service port", type=int)
parser.add_argument('-H', '--host', help="service IP address", default=socket.gethostname())

args = parser.parse_args()

if (args.port is not None) and (args.host is not None):
    server = Server(args.port, args.host)
    server.main_loop()
else:
    print("Paramters missing")
    exit()
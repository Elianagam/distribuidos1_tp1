import argparse
import socket
from server import ServerTCP

parser = argparse.ArgumentParser()

parser.add_argument('-p', '--port', help="service port", type=int)
parser.add_argument('-h', '--host', help="service IP address", default=socket.gethostname())

args = parser.parse_args()

if (args.port is not None) and (args.host is not None):
    server = ServerTCP(args.port, args.host)
    server.main_loop()
else:
    print("Paramters missing")
    exit()
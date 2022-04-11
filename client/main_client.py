import argparse
import socket
from client import Reporter

parser = argparse.ArgumentParser()

parser.add_argument('-p', '--port', help="server port", type=int, required=True)
parser.add_argument('-H', '--host', help="host server IP address", default=socket.gethostname())
parser.add_argument('-m', '--mode', help="report / agg / consumer", default = 'consumer')
parser.add_argument('-i', '--id', help="metric Id")
parser.add_argument('-v', '--value', help="Metric Value")

args = parser.parse_args()


if args.port is not None and args.host is not None \
	and args.mode == "report" \
	and args.id is not None and args.value is not None:
    	client = Reporter(args.host, args.port)
    	client.run(args.id, args.value)
else:
    print("Paramters missing")
    exit()
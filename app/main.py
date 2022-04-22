import logging
import signal
import os

from alerts.alert_handler import AlertHandler
from configparser import ConfigParser
from request_handler import RequestHandler
from threading import Event



def initialize_log(logging_level):
	"""
	Python custom logging initialization

	Current timestamp is added to be able to identify in docker
	compose logs the date when the log has arrived
	"""
	logging.basicConfig(
		format='%(asctime)s %(levelname)-8s %(message)s',
		level=logging_level,
		datefmt='%Y-%m-%d %H:%M:%S',
	)


def initialize_config():
	""" Parse env variables or config file to find program config params

	Function that search and parse program configuration parameters in the
	program environment variables first and the in a config file. 
	If at least one of the config parameters is not found a KeyError exception 
	is thrown. If a parameter could not be parsed, a ValueError is thrown. 
	If parsing succeeded, the function returns a ConfigParser object 
	with config parameters
	"""

	config = ConfigParser(os.environ)
	# If config.ini does not exists original config object is not modified
	config.read("app/config.ini")

	config_params = {}
	try:
		config_params["port"] = int(config["DEFAULT"]['server_port'])
		config_params["listen_backlog"] = int(config["DEFAULT"]["server_listen_backlog"])
		config_params["logging_level"] = config["DEFAULT"]["logging_level"]
		config_params["queue_size"] = int(config["DEFAULT"]["queue_size"])
		config_params["n_workers"] = int(config["DEFAULT"]["n_workers"])
		config_params["time_alert"] = int(config["DEFAULT"]["time_alert"])

	except KeyError as e:
		raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
	except ValueError as e:
		raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

	return config_params

def stop_event_handler(stop_event):
	logging.info("---- Close after SIGTERM signal...")
	stop_event.set()


def main():
	try:
		config_params = initialize_config()
		initialize_log(config_params["logging_level"])

		# Log config parameters at the beginning of the program to verify the configuration
		# of the component
		logging.debug("Server configuration: {}".format(config_params))

		stop_event = Event()
		signal.signal(signal.SIGTERM, lambda signal, frame: stop_event_handler(stop_event))

		# Initialize server and start server loop
		request_handler = RequestHandler(config_params["port"], config_params["listen_backlog"], 
			config_params["queue_size"], stop_event, config_params["n_workers"])
		request_handler.start()

		timer_event = Event()
		alert_handler = AlertHandler(config_params["queue_size"], stop_event, 
			timer_event, config_params["time_alert"], config_params["n_workers"])
		alert_handler.start()

	except (KeyboardInterrupt, SystemExit):
		logging.info(f"[MAIN_SERVER] Stop event is set")
		#stop_event.set()
		request_handler.join()
		alert_handler.join()


if __name__ == "__main__":
	main()
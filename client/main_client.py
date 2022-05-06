import logging
import os
import random
import time

from configparser import ConfigParser
from query_client import QueryClient
from reporter_client import ReporterClient
from threading import Thread


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
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(config["DEFAULT"]["client_port"])
        config_params["host"] = config["DEFAULT"]["client_host"]
        config_params["logging_level"] = config["DEFAULT"]["logging_level"]
        config_params["mode"] = config["DEFAULT"]["client_mode"]

        # TODO ADD MORE PARAMETERS
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params




def send_multiples_reports(config_params):
    metrics = list(range(2,20))
    clients = []

    for i in range(5):
        metric_id = random.choice(metrics)
        value = float(random.randint(0, 50))
        metric = {"metric_id": str(metric_id), "value": value}
        
        client = ReporterClient(config_params["host"], config_params["port"]) 
        clients.append(client)
        client.run(metric)
        time.sleep(1)


def send_multiples_querys(config_params):
    metrics = list(range(1,6))
    windows = list(range(0,100, 10))
    aggregation = ["SUM", "MAX", "MIN", "COUNT"]

    clients = []

    for i in range(1):
        time.sleep(1)
        metric_id = random.choice(metrics)
        agg_op = random.choice(aggregation)
        win_sec = float(random.choice(windows))

        query = {"metric_id": str(1),
                    "from_date":"2022-04-24 09:46:00",
                    "to_date":"2022-04-24 09:55:00",
                    "aggregation": agg_op,
                    "aggregation_window_secs": win_sec
                    }

        client = QueryClient(config_params["host"], config_params["port"])
        clients.append(client)
        client.run(query)


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug("Client configuration: {}".format(config_params))

    # Initialize server and start server loop
    reports = Thread(target=send_multiples_reports, args=(config_params,))
    querys = Thread(target=send_multiples_querys, args=(config_params,))

    reports.start()
    querys.start()

    #reports.join()
    #querys.join()



if __name__ == "__main__":
    main()
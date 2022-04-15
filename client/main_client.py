import os
import logging
from configparser import ConfigParser
from reporter import Reporter
from aggregation_query import AggregationQuery


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
    config.read("client/config.ini")

    config_params = {}
    try:
        config_params["port"] = int(config["DEFAULT"]["client_port"])
        config_params["host"] = config["DEFAULT"]["client_host"]
        config_params["logging_level"] = config["DEFAULT"]["logging_level"]
        config_params["mode"] = config["DEFAULT"]["client_mode"]
        config_params["id"] = config["DEFAULT"]["metric_id"]
        config_params["value"] = float(config["DEFAULT"]["metric_value"])

        # TODO ADD MORE PARAMETERS
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params


def main():
    config_params = initialize_config()
    initialize_log(config_params["logging_level"])

    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    logging.debug("Client configuration: {}".format(config_params))

    # Initialize server and start server loop
    if config_params["mode"] == "report":
        client = Reporter(config_params["host"], config_params["port"])
        client.run(config_params["id"], config_params["value"])
    
    elif config_params["mode"] == "aggregation":
        query = {"metric_id": "2",
                    "from_date":"2022-04-11 00:00:00",
                    "to_date":"2022-04-14 00:00:00",
                    "aggregation":"SUM",
                    "aggregation_window_secs":0.0
                    }
        client = AggregationQuery(config_params["host"], config_params["port"])
        client.run(query)

    else:
        logging.error("[MAIN_CLIENT] Invalid mode")



if __name__ == "__main__":
    main()
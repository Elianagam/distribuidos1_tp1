import logging
from reporter_client import ReporterClient
from query_client import QueryClient
import random
import time


def send_multiples_reports():
	metrics = list(range(1,6))
	for i in range(50):
		time.sleep(2)
		metric_id = random.choice(metrics)
		value = float(random.randint(0, 50))
		metric = {"metric_id": metric_id, "value": value}
		
		client = ReporterClient(config_params["host"], config_params["port"])		
		client.run(metric)
		client.join()


def send_multiples_querys():
	metrics = list(range(1,6))
	windows = list(range(0,100, 10))
	aggregation = ["SUM", "MAX", "MIN", "COUNT"]

	for i in range(50):
		time.sleep(2)
		metric_id = random.choice(metrics)
		agg_op = random.choice(aggregation)
		win_sec = float(random.choice(windows))

		query = {"metric_id": metric_id,
                    "from_date":"2022-04-21 19:10:00",
                    "to_date":"2022-04-23 00:00:00",
                    "aggregation": agg_op,
                    "aggregation_window_secs": win_sec
                    }
        client = QueryClient(config_params["host"], config_params["port"])
        client.run(query)
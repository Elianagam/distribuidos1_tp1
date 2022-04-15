from threading import Lock
import csv
from datetime import datetime
from os.path import exists
import logging
from common.constants import DATE_FORMAT, METRIC_DATA_FILENAME


class MetricFileHandler():

	FIELDNAMES = ["metric_id", "value", "datetime"]

	def __init__(self):
		self._lock = Lock()


	def exists(self, metric_id):
		return exists(metric_id)


	def write(self, metric_data):
		self._lock.acquire()
		try:
			with open(METRIC_DATA_FILENAME.format(agg_req['metric_id']), "a") as file:
				writer = csv.DictWriter(file, fieldnames=self.FIELDNAMES)
				writer.writerow(metric_data)
		finally:
			self._lock.release()


	def read(self, agg_req):
		self._lock.acquire()
		try:
			with open(METRIC_DATA_FILENAME.format(agg_req['metric_id']), "r") as _file:
				rows = csv.DictReader(_file, fieldnames=self.FIELDNAMES)
				agg = self.__agg_metrics(agg_req, rows)
				return agg
		finally:
			self._lock.release()


	def __is_between_date(self, agg_req, row):
		m_date = datetime.strptime(row["datetime"], DATE_FORMAT)
		start = datetime.strptime(agg_req["from_date"], DATE_FORMAT)
		end = datetime.strptime(agg_req["to_date"], DATE_FORMAT)
		return start <= m_date < end


	def __agg_metrics(self, agg_req, metrics):
		agg = []
		by_window = agg_req["aggregation_window_secs"] > 0

		for row in metrics:
			if self.__is_between_date(agg_req, row):
				if by_window:
					agg.append((float(row["value"]), row["datetime"]))
				else:
					agg.append(float(row["value"]))

		if not by_window:
			# apply operation agg all data values
			return self.__apply_aggregation(agg_req["aggregation"], agg)
		else:
			self.__aggregation_by_window(agg_req["aggregation_window_secs"], agg_req["aggregation"], agg)


	def __apply_aggregation(self, op, metrics):
		if op == "MAX":
			return max(metrics)
		elif op == "MIN":
			return min(metrics)
		elif op == "COUNT":
			return len(metrics)
		elif op == "SUM":
			return sum(metrics)
		else:
			logging.error("[METRIC FILE HANDLER] Invalid Aggregation Operator")
			return None


	def __aggregation_by_window(self, w_size, op, metrics):
		# TODO
		pass

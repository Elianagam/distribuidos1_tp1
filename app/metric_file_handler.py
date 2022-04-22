import csv
import logging

from common.constants import DATETIME_FORMAT
from common.constants import FILEDATE_FORMAT
from common.constants import METRIC_DATA_FILENAME
from datetime import datetime
from datetime import timedelta
from os.path import exists
from threading import Lock


class MetricFileHandler:

	FIELDNAMES = ["metric_id", "value", "datetime"]

	def __init__(self):
		self._lock = Lock()


	def __exists(self, metric_id, sdate):
		return exists(METRIC_DATA_FILENAME.format(metric_id, sdate))


	def write(self, metric_data):
		self._lock.acquire()
		try:
			sdate = datetime.strptime(metric_data["datetime"], DATETIME_FORMAT).strftime(FILEDATE_FORMAT) 
			mid = metric_data['metric_id']
			filename = METRIC_DATA_FILENAME.format(mid, sdate)
			with open(filename, "a") as file:
				writer = csv.DictWriter(file, fieldnames=self.FIELDNAMES)
				writer.writerow(metric_data)

		finally:
			self._lock.release()


	def aggregate(self, agg_req):
		self._lock.acquire()
		
		try:
			dates_between = self.__dates_between(agg_req["to_date"], agg_req["from_date"])
			all_data = []
			for sdate in dates_between:
				filename = METRIC_DATA_FILENAME.format(agg_req['metric_id'], sdate)
				if exists(filename):
					with open(filename, "r") as _file:
						rows = csv.DictReader(_file, fieldnames=self.FIELDNAMES)
						for data in rows:
							all_data.append(data)

			if (all_data == []): return "Empty data"
			return self.__split_data(agg_req, all_data)
		
		except Exception as e:
			log.error(f"[AGGREGATE_FILE] Error: {e}")

		finally:
			self._lock.release()


	def check_limit(self, limit_req):
		self._lock.acquire()
		dates_between = self.__dates_between(limit_req["to_date"], limit_req["from_date"])
		all_data = []
		try:
			for sdate in dates_between:
				filename = METRIC_DATA_FILENAME.format(limit_req['metric_id'], sdate)
				if exists(filename):
					with open(filename, "r") as _file:
						rows = csv.DictReader(_file, fieldnames=self.FIELDNAMES)
						for data in rows:
							all_data.append(data)
				
			agg_data = self.__split_data(limit_req, all_data)
			limit_exceded = self.__is_exceded(agg_data, limit_req["limit"])
				
			if limit_exceded:
				return {"limit_exceded": agg_data, "alert": limit_req}
			return None

		except Exception as e:
			log.error(f"[CHECK_LIMIT_FILE] Error: {e}")

		finally:
			self._lock.release()


	def __is_exceded(self, result, limit):
		if result == None or result == []: return False

		if type(result) == list and len(result) > 1:
			for windows in result:
				for value in windows:
					if float(value) > float(limit):
						return True
			return False

		# Si no es por ventanas es una lista de un unico elemento
		return result > float(limit)
		

	def __split_data(self, agg_req, metrics):
		agg_data = []
		by_window = float(agg_req["aggregation_window_secs"]) > 0

		for row in metrics:
			if self.__is_between_date(agg_req["from_date"], agg_req["to_date"], row["datetime"]):
				if by_window == True:
					agg_data.append( (float(row["value"]), row["datetime"]) )
				else:
					agg_data.append(float(row["value"]))
		if by_window:
			return self.__aggregation_by_window(agg_req["aggregation_window_secs"], agg_req["aggregation"], agg_data)
		else:
			# apply operation agg all data values
			return self.__apply_aggregation(agg_req["aggregation"], agg_data)


	def __aggregation_by_window(self, w_size, op, metrics):
		# metrics: tuple (value, date)
		try:
			start, end = self.__start_end_window(metrics[0][1], w_size)
			split_by_window = [ [metrics[0][0]] ]
			
			for value,sdate in metrics[1:]:
				m_date = sdate#self.__string_to_date(sdate)
				if self.__is_between_date(start, end, m_date):
					# append element to last windown_bucket in list
					split_by_window[len(split_by_window) - 1].append(value)

				else: 
					start, end = self.__start_end_window(sdate, w_size)
					# append a new window_bucket
					split_by_window.append( [value] )

			agg_result = []
			for bucket in split_by_window:
				agg_result.append(self.__apply_aggregation(op, bucket))
			
			return agg_result
		except Exception as e:
			logging.error(f"[METRIC_FILE_HANDLER] Error in aggregate by window: {e}")
			return "ERROR"


	def __dates_between(self, to_date, from_date):
		if type(from_date) == str:
			from_date = self.__string_to_date(from_date)
			to_date = self.__string_to_date(to_date)

		delta = to_date - from_date   # returns timedelta
		minutes = divmod(delta.seconds, 60)[0]

		dates_between = []		
		for i in range(minutes + 1):
			dmin = from_date + timedelta(minutes=i)
			dates_between.append( dmin.strftime(FILEDATE_FORMAT) )

		return dates_between


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
			logging.error("[METRIC_FILE_HANDLER] Invalid Aggregation Operator")
			return None


	def __is_between_date(self, from_date, to_date, actual_date):
		if type(from_date) == str:
			from_date = self.__string_to_date(from_date)
			to_date = self.__string_to_date(to_date)

		m_date = self.__string_to_date(actual_date)
		return from_date <= m_date < to_date


	def __string_to_date(self, sdate):
		return datetime.strptime(sdate, DATETIME_FORMAT)


	def __start_end_window(self, sdate, w_size):
		try:
			start_win_date = self.__string_to_date(sdate)
			end_win_date =  self.__string_to_date(sdate) + timedelta(seconds=w_size)
			return start_win_date, end_win_date
		except Exception as e:
			logging.error(f"[METRIC_FILE_HANDLER] Error in create window_bucket date: {e}")
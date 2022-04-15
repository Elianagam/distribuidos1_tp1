from datetime import datetime
from common.constants import DATE_FORMAT


class Request:
	def __init__(self, dict_data):
		  
		for key,value in dict_data.items():
			setattr(self, key, value)

class ReportMetric(Request):

	def __init__(self, dict_data):
		super().__init__(dict_data)

	def is_valid(self):
		try:
			return (type(self.metric_id) is str) and (type(self.value) is float)
		except:
			return False


class AggregationQuery(Request):

	def __init__(self, dict_data):
		super().__init__(dict_data)

	def is_valid(self):
		try:
			check_data =  (type(self.metric_id) is str) \
				and (type(self.aggregation) is str) \
				and (type(self.aggregation_window_secs) is float)

			# Checkea si el formato fecha es correcto
			from_date = datetime.strptime(self.from_date, DATE_FORMAT)
			to_date = datetime.strptime(self.to_date, DATE_FORMAT)
			return check_data and True
		except e:
			logging.error(f"[AGGREGATION QUERY] {e}")
			return False
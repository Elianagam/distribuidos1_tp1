from common.constants import DATETIME_FORMAT
from datetime import datetime


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
			from_date = datetime.strptime(self.from_date, DATETIME_FORMAT)
			to_date = datetime.strptime(self.to_date, DATETIME_FORMAT)
			return check_data and True
		except:
			logging.error(f"[AGGREGATION QUERY] Error en el formato de las fechas")
			return False
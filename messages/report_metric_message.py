import json
from datetime import datetime


class ReportMetricMessage:
	def __init__(self, metric_id, value):
		self.metric_id = metric_id
		self.value = value
		self.datetime = datetime.now().strftime("%Y-%M-%d %H:%M:%S")

	def serialize(self):
		return json.dumps(self.__dict__)
import json

class ReportMetricMessage:
	def __init__(self, metric_id, value):
		self.metric_id = metric_id
		self.value = value

	def serialize(self):
		return json.dumps(self.__dict__)
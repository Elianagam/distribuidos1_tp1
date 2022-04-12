from threading import Lock
import csv


class SafeFileWriter():

	def __init__(self, filename):
		self._lock = Lock()
		self._filename = filename

	def write(self, metric_data):
		self._lock.acquire()
		with open("./data/" + self._filename, "a") as _file:
			_writer = csv.DictWriter(_file, fieldnames=["metric_id", "value", "datetime"])
			print(metric_data)
			_writer.writerow(metric_data)

		self._lock.release()

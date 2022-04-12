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
			_writer.writerow(metric_data)

		self._lock.release()

	def read(self):
		rows = []
		with open("./data/" + self._filename, "r") as _file:
			reader = csv.DictReader(_file, fieldnames=["metric_id", "value", "datetime"])
			next(reader, None)
			for row in reader:
				rows.append(row)
		return rows

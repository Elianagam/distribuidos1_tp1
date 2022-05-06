from multiprocessing import Lock


class ClientCounter():
	def __init__(self, count=0):
		self.count = count
		self.lock = Lock()

	def increment(self):
		with self.lock:
			self.count += 1

	def decrement(self):
		with self.lock:
			self.count -= 1

	def value(self):
		with self.lock:
			return self.count
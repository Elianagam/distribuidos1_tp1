import fcntl
import logging

class Filelock:
	def acquire(self, mode="r"):
		try:
			file = open(filepath, mode)
			if mode == "r":
				fcntl.lockf(file, fcntl.LOCK_SH)
			elif mode in ["a", "w"]:
				fcntl.lockf(file, fcntl.LOCK_EX)
			return file
		except Exception as e:
			logging.error(f"[FILELOCK] Error: {e}")


	def release(self, lock):
		fcntl.flock(lock, fcntl.LOCK_UN)
import fcntl
import logging

class Filelock:
	def acquire(self, filename, mode="r"):
		try:
			file = open(filename, mode)
			if mode == "r":
				fcntl.lockf(file, fcntl.LOCK_SH)
			elif mode in ["a", "w"]:
				fcntl.lockf(file, fcntl.LOCK_EX)
			return file
		except Exception as e:
			logging.error(f"[FILELOCK] Error: {e}")


	def release(self, lockfile):
		fcntl.flock(lockfile, fcntl.LOCK_UN)
		lockfile.close()
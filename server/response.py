import json

class Response:
	def __init__(self, status, msg):
		self.status = status
		self.msg = msg

	# TODO: Replace for fctory

	def serialize(self):
		return json.dumps(self.__dict__)


class ServerError(Response):
	STATUS = 500
	MSG = "ERROR - Servidor no disponible. Intente más tarde"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)


class ValidMode(Response):
	STATUS = 200
	MSG = "OK - Modo valido"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)


class InvalidMode(Response):
	STATUS = 404
	MSG = "ERROR - Modo invalido"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)


class SuccessAggregation(Response):
	STATUS = 200
	MSG = "OK - Query procesada correctamente."

	def __init__(self, agg_array):
		super().__init__(self.STATUS, self.MSG)
		self.agg = agg_array

	def serialize(self):
		dict_response = {"status": self.STATUS, "msg": self.MSG, "agg": self.agg}
		return json.dumps(dict_response)


class MetricIdNotFound(Response):
	STATUS = 404
	MSG = "ERROR - MetricId recibido no existe"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)


class BadRequest(Response):
	STATUS = 400
	MSG = "ERROR - Formato de Query incorrecto"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)

class SuccessRecv(Response):
	STATUS = 200
	MSG = "OK - Metrica recibida correctamente"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)


class BadRequest(Response):
	STATUS = 400
	MSG = "ERROR - Formato de metrica incorrecto"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)


class CloseMessage(Response):
	STATUS = 200
	MSG = "close"

	def __init__(self):
		super().__init__(self.STATUS, self.MSG)
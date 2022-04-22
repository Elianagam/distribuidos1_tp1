def int_to_bytes(number):
	return number.to_bytes(2, byteorder='big')

def int_from_bytes(xbytes):
    return int.from_bytes(xbytes, 'big')

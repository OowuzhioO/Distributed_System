import json
import socket

RING_PORT = 2222
CONTACT_PORT = 3333
HOSTS = ['fa17-cs425-g48-'+ "%02d"%machine_num +'.cs.illinois.edu' for machine_num in range(1,11)]
CONTACT_HOST = HOSTS[0]
BYTES_INT = 16

def encoded(mem_list):
	return json.dumps(mem_list)

def decoded(message):
	return json.loads(message)

# helper function: get length from header
def getLength(sock):
	received = 'enter_loop' 
	result = "" # cumulative
	while (len(received) > 0 and len(result) < BYTES_INT):
		received = sock.recv(min(BYTES_INT-len(result), SIZE))
		result += received
	return 0 if (len(result) < BYTES_INT) else int(result)

	
def receiveAllToFile (sock, target_file): # do only at the end
	len_left_over = getLength(sock)	
	received = 'enter_loop' 
	while (len(received) > 0 and len_left_over > 0): 
		received = sock.recv(min(len_left_over, SIZE))
		target_file.write(decoded(received))
		len_left_over -= len(received)


def receiveAll(sock): #protocol part, check length
	length = getLength(sock)	
	received = 'enter_loop' 
	result = ""
	while (len(received) > 0 and len(result) < length):
		#print(len(result), length)
		received = sock.recv(min(length-len(result),SIZE))
		result += received
	return decoded(result) 


def sendAll(sock, message):
	encoded_m = encoded(message)
	length_str = str(len(encoded_m))
	if len(length_str) > BYTES_INT:
		print('Message too long to send, exit...')
		sys.exit()
	sock.sendall(length_str.zfill(BYTES_INT))
	sock.sendall(encoded_m)

def middle_man(read_sock, write_sock): 
	return None

import json
import socket
import os

SERVER_PORT = 3255
HOSTS = ['fa17-cs425-g48-'+ "%02d"%machine_num +'.cs.illinois.edu' for machine_num in range(1,11)]
BYTES_INT = 16 # limit the max file size
SIZE = 8192 # limit each packet receive size

def encoded(message):
	return json.dumps(message)

def decoded(message):
	return json.loads(message)



def return_all_received(sock, length):
	received = 'enter_loop'
	result = ""
	while (len(received) > 0) and (len(result) < length):
		received = sock.recv(min(length-len(result), SIZE))
		result += received	
	return result
		
def get_length(sock):
	result = return_all_received(sock, BYTES_INT)
	return 0 if (len(result) < BYTES_INT) else int(result)

# return the first complete message for process
def receive_all_decrypted(sock):
	return decoded(return_all_received(sock, get_length(sock)))

# send the first complete message instead of return
# an optional argument target is included for sending to another destination (relaying)
def receive_all_to_target(sock, target = None):
	file_name = receive_all_decrypted(sock)
	len_left_over = get_length(sock)
	if type(target) == socket._socketobject:
		send_all_encrypted(file_name)
		target.sendall(str(len_left_over).zfill(BYTES_INT))
	else: # don't replay, send to file
		target = open(file_name, 'w')
	received = 'enter_loop'

	while (len(received) > 0 and len_left_over > 0):
		received = sock.recv(min(len_left_over, SIZE))
		if type(target) == socket._socketobject:
			target.sendall(received)
		else: # file 
			target.write(received)

	if type(target) != socket._socketobject:
		target.close()
	return str(file_name)



def fill_header(message_length):
	length_str = str(message_length)		
	if len(length_str) > BYTES_INT:
		print('Message too long to send, exit...')
		sys.exit()
	return length_str.zfill(BYTES_INT)
	
# send the encrypted message along with header to the target sock
def send_all_encrypted(sock, message):
	encoded_m = encoded(message)
	header = fill_header(len(encoded_m))
	sock.sendall(header) 
	sock.sendall(encoded_m)


# send the whole file content piece by piece without encryption
def send_all_from_file(sock, file_name):
	send_all_encrypted(sock, file_name)
	with open(file_name,'r') as f:
		file_content = f.read()
	header = fill_header(len(file_content))
	sock.sendall(header)
	sock.sendall(file_content)

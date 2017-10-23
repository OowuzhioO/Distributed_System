import socket
import sys

# Use vm\*.log to escape regex match
PORT = 1245
HOSTS = ['fa17-cs425-g48-'+ "%02d"%machine_num +'.cs.illinois.edu' for machine_num in range(1,11)]

SIZE = 256
BYTES_INT = 8 # how many bytes for a string form int

def getLength(sock):
	received = 'enter_loop' 
	result = "" # cumulative
	while (len(received) > 0 and len(result) < BYTES_INT):
		received = sock.recv(min(BYTES_INT-len(result), SIZE))
		result += received
	return 0 if (len(result) < BYTES_INT) else int(result)

	
def receiveAllToFile (sock, target_file): # do only at the end
	received = 'enter_loop' 
	while (len(received) > 0): # end when server ends the connection
		received = sock.recv(SIZE)
		target_file.write(received)


def receiveAll(sock): #protocol part, check length
	length = getLength(sock)	
	received = 'enter_loop' 
	result = ""
	while (len(received) > 0 and len(result) < length):
		#print(len(result), length)
		received = sock.recv(min(length-len(result),SIZE))
		result += received
	return result.decode('utf-8')


def sendAll(sock, message):
	encoded = message.encode('utf-8')
	length_str = str(len(encoded))
	if len(length_str) > BYTES_INT:
		print('Message too long to send, exit...')
		sys.exit()
	sock.sendall(length_str.zfill(BYTES_INT))
	sock.sendall(encoded)


import threading 
thread_num_lines = [0]*10
threads = [None]*10

def thread_receive(machine_ix):
	host = HOSTS[machine_ix]
	client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_ip = socket.gethostbyname(host)
	try:
		client_sock.connect((server_ip, PORT))
	except:
		# print('no server')
		return

	#print(sys.argv[1:])
	sendAll(client_sock,' '.join(sys.argv[1:]))
	#print('machine'+ntohl(receiveAll(client_sock)))
	with open('grep_machine%d.log'%(machine_ix+1), 'w') as grep_f:
		thread_num_lines[machine_ix] = int(receiveAll(client_sock))
		receiveAllToFile(client_sock, grep_f) 

if __name__ == "__main__":
	for ix in range(10):
		threads[ix] = threading.Thread(target=thread_receive, args=(ix,))
		threads[ix].start()

	for t in threads:
		t.join()

	for i,l in enumerate(thread_num_lines): 
		print('vm{}: {}'.format(i+1, l))
	print('Total: {}'.format(sum(thread_num_lines)))
		


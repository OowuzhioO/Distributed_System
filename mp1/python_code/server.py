import socket
import sys
from client import PORT, HOSTS, receiveAll, sendAll
import subprocess

reload(sys)  
sys.setdefaultencoding('utf8')

server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
	server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	server_sock.bind((socket.gethostname(), PORT))
except socket.error, (message):
	print ('Bind failed: '+str(message))
	sys.exit()

server_sock.listen(5)

while True:
	conn_sock, addr = server_sock.accept()
	if socket.gethostbyaddr(addr[0])[0] not in HOSTS:
		continue
	partial_command = receiveAll(conn_sock)
	command = 'grep '+partial_command
	print(command)
	#command_count = 'grep --count '+partial_command
	#process_count = subprocess.Popen(command_count, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
	#sendAll(conn_sock, process_count.communicate()[0])

	process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
	lines = process.stdout.readlines()
	sendAll(conn_sock, str(len(lines)))
	conn_sock.sendall(''.join(lines))

	# Experiment: can't readlines and communicate together
	# otherwise the second result would be empty

	#process.wait()
	#for line in process.stdout:
		#conn_sock.sendall(line)
	conn_sock.close()


		


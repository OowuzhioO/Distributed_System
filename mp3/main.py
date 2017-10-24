import socket
import sys

HOST, PORT = socket.gethostname(), 9999
data = " ".join(sys.argv[1:])

# Create a socket (SOCK_STREAM means a TCP socket)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
	# Connect to server and send data
	sock.connect((HOST, PORT))
	sock.sendall(data + "\n")
	print(type(sock))
	with open('read.py') as a:
		print(type(a))
		print(isinstance(a, socket._socketobject))
		print(isinstance(sock, socket._socketobject))
		print(isinstance(a, file))
		print(isinstance(sock, file))

	# Receive data from the server and shut down
	received = sock.recv(1024)
finally:
	sock.close()

print "Sent:	 {}".format(data)
print "Received: {}".format(received)

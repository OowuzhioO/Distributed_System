import SocketServer
from message import *
import socket

class MyTCPHandler(SocketServer.StreamRequestHandler):
	"""
	The request handler class for our server.

	It is instantiated once per connection to the server, and must
	override the handle() method to implement communication to the
	client.
	"""

	def handle(self):
		message_type, file_name = receive_all_decrypted(self.request)
		if (message_type == 'read'):
			return # implement	
		elif (message_type == 'write'):
			return # implement
		elif (message_type == 'delete'):
			return # implememnt

if __name__ == "__main__":
	HOST, PORT = socket.gethostname(), SERVER_PORT

	# Create the server, binding to localhost on port 9999
	SocketServer.ThreadingTCPServer.allow_reuse_address = 1
	server = SocketServer.ThreadingTCPServer((HOST, PORT), MyTCPHandler)
	# needs membership list
	server.conn = (None)

	# Activate the server; this will keep running until you
	# interrupt the program with Ctrl-C
	server.serve_forever()

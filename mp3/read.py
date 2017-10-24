import SocketServer
import socket

class MyTCPHandler(SocketServer.StreamRequestHandler):
	"""
	The request handler class for our server.

	It is instantiated once per connection to the server, and must
	override the handle() method to implement communication to the
	client.
	"""

	def handle(self):
		# self.rfile is a file-like object created by the handler;
		# we can now use e.g. readline() instead of raw recv() calls
		print('connection',self.server.conn)
		self.data = self.rfile.readline().strip()
		print "{} wrote:".format(self.client_address[0])
		print self.data
		# Likewise, self.wfile is a file-like object used to write back
		# to the client
		self.wfile.write(self.data.upper())


if __name__ == "__main__":
	HOST, PORT = socket.gethostname(), 9999

	# Create the server, binding to localhost on port 9999
	SocketServer.ThreadingTCPServer.allow_reuse_address = 1
	server = SocketServer.ThreadingTCPServer((HOST, PORT), MyTCPHandler)
	server.conn = ("10.0.0.5",  "user", "pass", "database")


	# Activate the server; this will keep running until you
	# interrupt the program with Ctrl-C
	server.serve_forever()

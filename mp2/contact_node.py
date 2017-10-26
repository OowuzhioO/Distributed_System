import SocketServer
import socket
import sys
from datetime import datetime
from message import *
import time

# Contact handler that introduces new nodes to the group
class ContactUDPHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		host = socket.gethostbyaddr(self.client_address[0])[0] 
		# The index is used to determine its neighbors
		host_ix = HOSTS.index(host)
		targets = [h for h in HOSTS if h != host]
		data = decoded(self.request[0].strip())
		# Generate an ID as <hostname>,<datetime>
		new_id = '%s,%s' % (host,datetime.now())
		# Let the joining node know its assigned ID and then gossip that to its neighbors
		self.request[1].sendto(encoded(new_id), self.client_address)
		time.sleep(0.5)
		gossip_mem_list(targets, RING_PORT, {new_id: 0})  	

if __name__ == '__main__':
	if socket.gethostname() != CONTACT_HOST:
		print('Error: contact host must be: %s'%CONTACT_HOST)
		sys.exit()

	server = SocketServer.UDPServer(('', CONTACT_PORT), ContactUDPHandler)
	server.allow_reuse_address=True
	server.serve_forever()

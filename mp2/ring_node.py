import SocketServer, socket
import random
from message import *
import threading
import sys,time,signal
import threading
from logger import *

# Tfail to suspect a node and then mark it as failed
MAX_TOLERANCE = 4

membership_list = {} # <hostname>,<timestamp>:clock
mem_list_lock = threading.Lock() # synchronize clock
clock = 0
assigned_id = 0
neighbors = []

# Parse the hostname from the ID
def hostname(id):
	return id.split(',')[0]

class RingUDPHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		data = decoded(self.request[0].strip())
		sock = self.request[1]
		To_gossip = False

		with mem_list_lock:
			# Update the membership list based on the received gossip
			for node_id,timestamp in data.items():
				# A node has been marked as suspicious. Forward it to the gossiper
				if node_id in membership_list and (timestamp > 0) == (membership_list[node_id] <= 0): 
					membership_list[node_id] = min(timestamp, membership_list[node_id])
				# Update the heartbeat entry
				elif node_id in membership_list:
					membership_list[node_id] = max(timestamp, membership_list[node_id]) 
				# Insert a new member into the membership list
				else:
					global clock
					clock = max(clock, timestamp) # pick the most up-to-date time
					membership_list[node_id] = clock
					log_join(hostname(node_id))
					To_gossip = True
			# Dissemination
			if To_gossip:
				gossip_mem_list(neighbors, RING_PORT, membership_list)

# Receive and handle messages
def run_server():
	server = SocketServer.UDPServer(('', RING_PORT), RingUDPHandler)
	server.allow_reuse_address=True
	server.serve_forever()

def leaving_handler(signal, frame):
	log_leave(socket.gethostname())
	membership_list[assigned_id] = -MAX_TOLERANCE*2 # Mark it distinctly that it's leaving
	gossip_mem_list(HOSTS, RING_PORT, {assigned_id: -MAX_TOLERANCE*2})
	sys.exit()

def failing_handler(signal, frame):								  
	log_fail(socket.gethostname())									  
	sys.exit()

# Join the group to begin receiving receiving and sending heartbeats
def receive_info_from_server():
	# Establish connection with the contact server to join the group
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.sendto(encoded(None), (CONTACT_HOST, CONTACT_PORT))
	data = decoded(sock.recv(1024))
	return data
	
# Periodically send heartbeats or disseminate status updates
def gossip():
	# Send heartbeats while this program is still alive
	while True:
		# Synchronize membership heartbeating
		with mem_list_lock:
			global clock 
			clock += 1
			# Find all nodes that have timed out
			nodes_to_delete = []
			for i in membership_list:
					timestamp = membership_list[i]
					if timestamp < 0 or i == assigned_id:
						# Update the time
						membership_list[i] += 1
						# Detect a leaving node
						if timestamp < -MAX_TOLERANCE:
							log_leave(hostname(i))
							nodes_to_delete.append(i)
						# Node has timed out again
						elif membership_list[i] == 0:
							log_fail(hostname(i))
							nodes_to_delete.append(i)
					# A timed out node is marked as suspicious until it responds or times out again
					elif clock-timestamp > MAX_TOLERANCE:
						# Mark suspicious if there's an initial timeout
						log_suspect(hostname(i))
						membership_list[i] = -MAX_TOLERANCE
			# delete nodes that are no longer in the membership
			for node in nodes_to_delete:
				del membership_list[node]
			# Send heartbeats to neighbors
			gossip_mem_list(neighbors, RING_PORT, membership_list)
			
		# Sleep before sending more heartbeats
		time.sleep(2./MAX_TOLERANCE)

# Display all members currently in the membership list
def print_membership():
	with mem_list_lock:
		for member in membership_list:
			print member

if __name__ == "__main__":
	# Initialize values specific for the node
	assigned_id = receive_info_from_server()
	host_idx = HOSTS.index(socket.gethostname())
	neighbors = [HOSTS[i%len(HOSTS)] for i in range(host_idx-2,host_idx+3) if i != host_idx]
	membership_list[assigned_id] = 0
	log_join(socket.gethostname())

	# Spawn a daemon thread to take care of receiving messages
	server_thread = threading.Thread(target=run_server, args=())
	server_thread.daemon = True
	server_thread.start()

	# Use signals to perform cleanup actions
	signal.signal(signal.SIGQUIT, leaving_handler) # CTRL-\ simulate node leaving
	signal.signal(signal.SIGINT, failing_handler) # CTRL-C simulate node failing/crash

	# Prompt
	print('Server started successfully.\nLeave: CTRL-\\ \nCrash: CTRL-C\nMembership List: m\nID: i')

	# Spawn a daemon thread to periodically gossip/send heartbeats
	gossip_thread = threading.Thread(target=gossip, args=())
	gossip_thread.daemon = True
	gossip_thread.start()

	# Allow to user to view the ID and membership list
	while True:
		command = raw_input('> ')
		if command == 'm':
			print_membership()
		elif command == 'i':
			print assigned_id
		else:
			print 'Invalid command'

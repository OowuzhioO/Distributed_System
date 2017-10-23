import json
import socket

RING_PORT = 2222
CONTACT_PORT = 3333
HOSTS = ['fa17-cs425-g48-'+ "%02d"%machine_num +'.cs.illinois.edu' for machine_num in range(1,11)]
CONTACT_HOST = HOSTS[0]

def encoded(mem_list):
	return json.dumps(mem_list)

def decoded(message):
	return json.loads(message)

# Gossips the membership list to all targets
def gossip_mem_list(targets, port, membership_list):
	for host in targets:
		send_UDP(host, port, membership_list)

# Encodes the membership list and sends it via UDP
def send_UDP(host, port, message):
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.sendto(encoded(message) + "\n", (host, port))

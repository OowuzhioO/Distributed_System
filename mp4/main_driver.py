from collections import OrderedDict,defaultdict
import logging
import socket
import threading
from multiprocessing import Process, Queue
import os, time, sys, argparse
from heartbeat import heartbeat_detector
from message import send_all_encrypted, send_all_from_file
from message import receive_all_decrypted, receive_all_to_target


### Heartbeat failure detector object ###
class Driver(object):
	#  added membList and changed host name, tFail
	def __init__(self, host_name, port, worker_port, master_port, membList, messageInterval=0.001):
		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.port = port
		self.worker_port = worker_port
		self.master_port = master_port
		self.membList = membList
		self.message_input = 'User has already inputted in the following machine'
		self.messageInterval = messageInterval

		self.role = 'unknown'
		self.filename = None

	def drive(self):
		newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
		queue = Queue()
		self.input_task = Process(target=self.get_input, args=(queue, newstdin,))
		self.input_task.daemon = True
		self.input_task.start()

		self.server_task = Process(target=self.background_server)
		self.server_task.daemon = True
		self.server_task.start()

		self.task_id, self.filename, self.role = queue.get()

		if (self.role == 'client'):
			self.start_as_client()

	def get_input(self, queue, newstdin):
		sys.stdin = newstdin
		input_ready = False
		while(not input_ready):
			try:
				task_id, filename = raw_input('Input task_id(0-PR, 1-SP) filename: ').strip().split()
				task_id = int(task_id)
			except:
				print 'Input should be in this form: 0 file'
				continue

			if (task_id > 1) or (task_id < 0):
				print 'Task id not in range 0 to 1 (0-PR, 1-SP)'
				continue

			elif not os.path.exists(filename):
				print 'File does not exist'
				continue

			else:
				input_ready = True

		queue.put((task_id, filename, 'client'))

	def background_server(self):
		# a monitor receive message, check and response, also multicase failure message
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.server_sock.bind((self.host, self.port))
		self.server_sock.listen(5)
		# self.monitor.listen(10) # UDP doesn't support this
		
		while True:
			conn, addr = self.server_sock.accept()				
			rmtHost= socket.gethostbyaddr(addr[0])[0]
			
			message = receive_all_decrypted(conn) # the instruction
			if message==self.message_input:
				self.input_task.terminate()

				self.masters_workers = [host.split('_')[0] for host in sorted(self.membList.keys()) if host.split('_')[0] != rmtHost]
				
				if self.host_name == self.masters_workers[0]:
					self.role = 'master'
					self.filename , _ = receive_all_to_target(conn, self.messageInterval)
					self.start_as_master()

				elif self.host_name == self.masters_workers[1]:
					self.role = 'standby'
					print 'I am the standby master!'

				else:
					self.role = 'worker'
					self.start_as_worker()

		# helper function to reduce code redundancy/duplication
	def getParams(self, target):
		target_hostname = target.split('_')[0]
		target_host = socket.gethostbyname(target_hostname)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((target_host, self.port)) # call might fail
		return target_host, sock 


	def start_as_client(self):
		print 'I am the client!'
		self.masters_workers = [host.split('_')[0] for host in sorted(self.membList.keys()) if host.split('_')[0] != self.host_name]

		for host_name in self.membList.keys():
			if host_name.split('_')[0] != self.host_name:
				target_host, sock = self.getParams(host_name)
				send_all_encrypted(sock, self.message_input)
			if len(self.masters_workers)>0 and host_name == self.masters_workers[0]:
				send_all_from_file(sock, filename, self.messageInterval)


	def start_as_master(self):
		print 'I am the master!'
		

	def start_as_worker(self):
		print 'I am the worker!'






#-------------------------------------------------------------------------------main-----------------------------------------------------------------------------------
if __name__ == '__main__':
	#attemp to add a port changing argument 
	ports = 2222, 3333, 4444, 5555, 6666, 7777

	parser = argparse.ArgumentParser()
	parser.add_argument("--port",'-p', type=int,default=8001)
	parser.add_argument("--verbose", '-v', action='store_true')
	parser.add_argument("--cleanLog", '-c', action='store_true')
	parser.add_argument("--messageInterval",'-i', type=float, default=0.001)
	parser.add_argument("--displayTime", '-d', action='store_true')



	args = parser.parse_args()
	# update VM ip with node id
	VM_DICT = {}
	VM_DICT.update(OrderedDict({'fa17-cs425-g48-%02d.cs.illinois.edu'%i:'Node%02d'%i for i in range(1,11)}))
	
	# manually assign two introducers
	VM_INTRO = ['Node01','Node02']


	# setup logger for membership list and heartbeating count
	# failure detector log directory
	FD_dir = './FD_log'
	if not os.path.exists(FD_dir): os.makedirs(FD_dir)
	FD_log_file = 'log.txt'
	FD_log_dir = os.path.join(FD_dir,FD_log_file)
	# create DS log collector if not exists
	if not os.path.exists(FD_log_dir):
		file = open(FD_log_dir, 'w+')
	elif args.cleanLog:
		os.remove(FD_log_dir)
		file = open(FD_log_dir, 'w+')

	loggingLevel = logging.DEBUG if args.verbose else logging.INFO
	logging.basicConfig(format='%(levelname)s:%(message)s', filename=FD_log_dir,level=loggingLevel)
	

	#Start FD program
	# a FD program should have 2 operations, join and leave the group
	#	upon join, this node to identify itself to one of active introducers, and recieve an active membership list of its topological neighbors
	#   upon leave, this node should send a clean up message to memebers on its list and mark itself as leave
	#   upon initlaization, introducer should setup the group
	hbd = heartbeat_detector(hostName=socket.gethostname(),
							VM_DICT=VM_DICT,
							tFail = 1.5,
							tick = 0.5,
							introList=VM_INTRO,
							port=ports[0],
							dfsPort=ports[1],
							num_N=3,
							randomthreshold = 0,
							messageInterval = args.messageInterval)

	monitor = threading.Thread(target=hbd.monitor)
	monitor.daemon=True
	monitor.start()
	hbd.joinGrp()

	main_driver = Driver(socket.gethostname(), ports[2], worker_port=ports[3], master_port=ports[4], membList=hbd.membList)
	main_driver.drive()




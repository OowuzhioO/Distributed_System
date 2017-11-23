from collections import OrderedDict,defaultdict
import logging
import socket
import threading
from multiprocessing import Process, Queue
import os, time, sys, argparse
from heartbeat import heartbeat_detector
from message import send_all_encrypted, send_all_from_file
from message import receive_all_decrypted, receive_all_to_target
from master import Master
from worker import Worker
from time import sleep

class Driver(object):
	def __init__(self, host_name, port, worker_port, master_port, membList, dfs, messageInterval, super_step_interval, result_file):
		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.port = port
		self.worker_port = worker_port
		self.master_port = master_port
		self.membList = membList
		self.dfs = dfs
		self.message_input = 'User has already inputted'
		self.message_output = 'I am done with processing file'
		self.messageInterval = messageInterval
		self.super_step_interval = super_step_interval
		self.result_file = result_file

		self.client_ip = None
		self.role = 'unknown'
		self.master = None # make sense only if role == 'master'
		self.filename_pair = [None, None]
		self.task_id = -1
		self.source = -1 # source vertex
		self.commons = ('file_piece_', 'Preprocess Done', 'Please Compute', 
						'Compute Done', 'Result?', 'Here are the results')

	def drive(self):
		newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
		queue = Queue()

        # a monitor receive message, check and response, also multicase failure message
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.server_sock.bind((self.host, self.port))
		self.server_sock.listen(5)

		self.input_task = Process(target=self.get_input, args=(newstdin, queue))
		self.input_task.daemon = True
		self.input_task.start()

		self.server_task = Process(target=self.background_server, args=(queue,))
		self.server_task.start()

		self.task_id, self.source, self.filename_pair, self.role, self.client_ip = queue.get()

		member_ips = [socket.gethostbyname(host.split('_')[0]) for host in sorted(self.membList.keys())]
		self.masters_workers = [host for host in member_ips if host != self.client_ip]
		print self.masters_workers
		if (self.role == 'client'):
			self.start_as_client()
		elif (self.role == 'master'):
			self.start_as_master()
		elif (self.role == 'worker'):
			self.start_as_worker()

	# assert no one fails during input time
	def get_input(self, newstdin, queue):
		sys.stdin = newstdin
		input_ready = False
		while(not input_ready):
			try:
				task_id, filename, source= raw_input('Input task_id(0-PR, 1-SP) filename source: ').strip().split()
				task_id = int(task_id)
			except:
				print 'Input should be in this format for PR: 0 file any_input'
				print 'Or in this format for SP: 1 file 105'
				continue

			if (task_id > 1) or (task_id < 0):
				print 'Task id not in range 0 to 1 (0-PR, 1-SP)'
				continue

			elif not os.path.exists(filename):
				print 'File does not exist'
				continue

			else:
				input_ready = True

		queue.put((task_id, source, (filename, self.result_file), 'client', self.host))

	def background_server(self, queue):
		conn, addr = self.server_sock.accept()				
		rmtHost= socket.gethostbyaddr(addr[0])[0]
		
		message = receive_all_decrypted(conn) # the instruction

		if message==self.message_input:
			self.input_task.terminate()
			print

			self.task_id = receive_all_decrypted(conn)

			real_members = [host.split('_')[0] for host in sorted(self.membList.keys())]
			self.masters_workers = [socket.gethostbyname(host) for host in real_members if host != rmtHost]
			
			if self.host == self.masters_workers[0]:
				self.role = 'master'
				self.filename_pair[0] , _ = receive_all_to_target(conn, self.messageInterval)
				self.filename_pair[1] = receive_all_decrypted(conn)
				self.source = receive_all_decrypted(conn)
				

			elif self.host == self.masters_workers[1]:
				self.role = 'standby'
				print 'I am the standby master!'

			else:
				self.role = 'worker'

			queue.put((self.task_id, None, self.filename_pair, self.role, addr[0]))


		elif message ==self.message_output: # for client
			filename, _ = receive_all_to_target(conn, self.messageInterval)
			assert(filename == self.result_file)
			print 'Task done, result is published to {}'.format(filename)


	def start_as_client(self):
		print 'I am the client!'
		real_members = [host.split('_')[0] for host in sorted(self.membList.keys())]
		self.masters_workers = [socket.gethostbyname(host) for host in real_members if host != self.host_name]

		for host in self.masters_workers:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((host, self.port))

			send_all_encrypted(sock, self.message_input)
			send_all_encrypted(sock, self.task_id)
			if host == self.masters_workers[0]:
				send_all_from_file(sock, self.filename_pair[0], self.messageInterval)
				send_all_encrypted(sock, self.filename_pair[1])
				send_all_encrypted(sock, self.source)


	def start_as_master(self):
		#self.master = Master
		print 'I am the master!'
		self.master = Master(self.membList, self.task_id, self.filename_pair, self.masters_workers, 
							self.host_name, (self.master_port, self.worker_port, self.port),(self.client_ip, self.message_output), 
							self.dfs, self.super_step_interval, self.commons)
		self.master.execute()

	def start_as_worker(self):
		print 'I am the worker!'
		self.worker = Worker(self.task_id, self.host_name, (self.master_port, self.worker_port), 
							self.masters_workers, self.source, self.dfs, self.commons)
		self.worker.start_main_server()



	def onProcessFail(self, failed_process):
		failed_process = socket.gethostbyname(failed_process.split('_')[0])
		if self.master != None and failed_process != self.client_ip:
			print('I care about '+failed_process)
			failed_ip = socket.gethostbyname(failed_process)



#-------------------------------------------------------------------------------main-----------------------------------------------------------------------------------
if __name__ == '__main__':
	#attemp to add a port changing argument 
	ports = 2222, 3333, 4444, 5555, 6666, 7777

	parser = argparse.ArgumentParser()
	parser.add_argument("--verbose", '-v', action='store_true')
	parser.add_argument("--cleanLog", '-c', action='store_true')
	parser.add_argument("--messageInterval",'-i', type=float, default=0.001)
	parser.add_argument("--output_file", '-o', type=str, default='processed_values.txt')
	parser.add_argument("--super_step", '-t', type=float, default='6.00')

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

	main_driver = Driver(socket.gethostname(), ports[2], ports[3],  ports[4], hbd.membList, hbd.file_sys, 
						args.messageInterval, args.super_step, args.output_file)
	hbd.fail_callback = main_driver.onProcessFail
	main_driver.drive()
	
	


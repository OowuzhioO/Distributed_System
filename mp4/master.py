import random 
from parser import parse_file, combine_files
from threading import Thread
from time import sleep, time
import json
from message import receive_all_decrypted, send_all_encrypted, send_all_from_file
import socket
from commons import Commons, dfsWrapper

class Master:

	def __init__(self, memblist, task_id, filename_pair, masters_workers, host_name, port_info, client_info, dfs):
		self.memblist = memblist
		self.task_id = task_id
		self.input_filename, self.output_filename = filename_pair
		self.masters_workers = masters_workers
		self.num_workers = len(masters_workers)-2

		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.master_port, self.worker_port, self.driver_port = port_info
		self.client_ip, self.client_message = client_info
		self.dfs = dfs

		self.num_preprocess_done = 0
		self.num_process_done = 0


	def send_to_worker(self, list_of_things, worker):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((worker, self.worker_port))
		send_all_encrypted(sock, list_of_things[0])
		send_all_encrypted(sock, list_of_things[1:])
		

	def background_server(self):
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.server_sock.bind((self.host, self.master_port))
		self.server_sock.listen(5)

		while True:
			conn, addr = self.server_sock.accept()				
			rmtHost= socket.gethostbyaddr(addr[0])[0]
			message = receive_all_decrypted(conn)

			if message == Commons.ack_preprocess:
				self.num_preprocess_done += 1

			elif message == Commons.finish_compute:	
				halt = receive_all_decrypted(conn)
				self.all_done.append(halt)

			elif message == Commons.ack_result:
				self.num_process_done += 1


	def preprocess(self):
		sleep(0.5)
		self.server_task = Thread(target=self.background_server)
		self.server_task.daemon = True
		self.server_task.start()

		print('I have {} workers!'.format(self.num_workers))
		self.v_to_m_dict, self.num_vertices = parse_file(self.input_filename, self.num_workers)
		print('num_vertices: ', self.num_vertices)

		dfsWrapper(self.dfs.putFile, self.input_filename)
		sleep(1.5)

		for ix in range(self.num_workers):
			self.send_to_worker([Commons.request_preprocess,self.input_filename, self.v_to_m_dict, self.num_vertices], self.masters_workers[ix+2])

		while (self.num_preprocess_done < self.num_workers):
			sleep(1)


	def process(self):
		self.superstep = 0
		self.all_done = False
		while not self.all_done:
			start_time = time()
			self.all_done = []	
			self.compute_count = 0
			self.superstep += 1
			for worker in self.masters_workers[2:]:
				self.send_to_worker([Commons.request_compute, self.superstep], worker)

			while (len(self.all_done) < self.num_workers):
				sleep(0.25)

			self.all_done = all(self.all_done)
			time_elapsed = time()-start_time
			print('Superstep {} ended after {} seconds...'.format(self.superstep, time()-start_time))


	def collect_results(self):
		self.result_files = [0]*self.num_workers

		for ix in range(self.num_workers):
			worker = self.masters_workers[ix+2]
			self.result_files[ix] = 'file_piece_'+str(ix)+'_out'
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			self.send_to_worker([Commons.request_result, self.result_files[ix]], worker)

		while (self.num_process_done < self.num_workers):
			sleep(1)

		for ix in range(self.num_workers):
			dfsWrapper(self.dfs.getFile,self.result_files[ix]) 

		combine_files(self.output_filename, self.result_files)

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.client_ip, self.driver_port))
		send_all_encrypted(sock, self.client_message)
		send_all_from_file(sock, self.output_filename, 0.001)

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.masters_workers[1], self.driver_port))
		send_all_encrypted(sock, self.client_message) # actually standby_message

		for worker in self.masters_workers[2:]:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			self.send_to_worker([Commons.end_now], worker)

		
	# execute the task in 3 phases
	def execute(self):
		if (self.num_workers < 1):
			print 'Error: No worker available'
			return

		start_time = time()
		self.preprocess()
		print('Preprocess done, took {} seconds'.format(time()-start_time))

		start_time = time()
		self.process()
		print('Process done, took {} seconds'.format(time()-start_time))

		start_time = time()
		self.collect_results()
		print('Results collected, took {} seconds'.format(time()-start_time))

	

from vertex import PRVertex, SPVertex
from message import send_all_encrypted, receive_all_decrypted
import threading
import json 
import sys, time
import socket
from collections import OrderedDict,defaultdict
from commons import Commons, dfsWrapper

class Worker(object):
	# host_name: hostname of machine
	# port_info: (master_port, worker_port)
	# task_id:  0 for pagerank and 1 for shortest path
	# masters_workers: master, standby, and workers in order
	# source_vertex: for shortest path
	# commons: information shared between Master and Worker
	
	def __init__(self, task_id, host_name, port_info, masters_workers, key_number, dfs, buffer_size):
		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.master_port, self.worker_port, self.vertex_port = port_info
		self.task_id = task_id 
		self.key_number = key_number
		self.buffer_size = buffer_size
		self.dfs = dfs
		self.vertices = {}
		self.targetVertex = PRVertex if (task_id==0) else SPVertex

		self.masters_workers = masters_workers
		self.num_workers = len(masters_workers)-2
		self.superstep = 0
		self.vertex_to_messages = defaultdict(list)
		self.vertex_to_messages_next = defaultdict(list)

		self.remote_message_buffer = defaultdict(list) # key are hosts, vals are params
		self.max_buffer_size = 666
		self.num_threads = 1 # should use process pool to not share memory ......

		# for debugging
		self.first_len_message = defaultdict(int)
		self.local_global = [0,0]

		# developing ......
		self.send_buffer_count = defaultdict(int)
		self.receive_buffer_count = defaultdict(int)
		self.buffer_count_received = defaultdict(int)

	def gethost(self, vertex):
		return self.masters_workers[2+self.v_to_m_dict[vertex]]

	def preprocess(self, filename):
		with open(filename, 'r') as input_file:
			for line in input_file.readlines():
				if line[0] < '0' or line[0] > '9':
					continue
				u, v = line.split()

				if self.gethost(u) == self.host:
			   		if u not in self.vertices:
						self.vertices[u] = self.targetVertex (u, [(v, 1, self.gethost(v))],
							self.vertex_send_messages_to,self.vertex_edge_weight, self.key_number, self.num_vertices)
					else:
						self.vertices[u].neighbors.append((v, 1, self.gethost(v)))

				if self.gethost(v) == self.host:
					self.first_len_message[v] += 1
				
	def queue_message(self, vertex, value, superstep):
		if self.superstep != superstep:
			assert(self.superstep == superstep-1)
			self.vertex_to_messages_next[vertex].append(value)
		else:
			self.vertex_to_messages[vertex].append(value)
			print('Doesn\'t follow current design though')
		# should use combinator but hard to implement to be thread-safe...

	def load_to_file(self, filename):
		with open(filename, 'w') as f:
			f.write(str(self.superstep)+'\n')
			for key in sorted(self.vertices.keys()):
				v = self.vertices[key]
				f.write(str(v.vertex)+' '+str(v.value)+'\n')

	def start_main_server(self):
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.worker_port))
		self.monitor.listen(5)

		while True:
			conn, addr = self.monitor.accept()
			message = receive_all_decrypted(conn)

			if message == Commons.request_preprocess:
				start_time = time.time()
				print('receive command to load file')
				self.addr = addr[0]
				self.input_filename, self.v_to_m_dict, self.num_vertices = receive_all_decrypted(conn)
				dfsWrapper(self.dfs.getFile, self.input_filename)
				self.preprocess(self.input_filename)
				print('preprocess done after {} seconds'.format(time.time()-start_time))

				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((self.addr, self.master_port))
				send_all_encrypted(sock, Commons.ack_preprocess)

			elif message == Commons.request_compute:
				superstep = receive_all_decrypted(conn)[0]
				threading.Thread(target=self.compute, args=(superstep,)).start()

			elif message == Commons.request_result: # final step
				self.output_filename = receive_all_decrypted(conn)[0]
				self.load_to_file(self.output_filename)
				dfsWrapper(self.dfs.putFile, self.output_filename)
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((self.addr, self.master_port))
				send_all_encrypted(sock, Commons.ack_result)

			elif message == Commons.end_now:
				sys.exit()

			elif message == None: # for inner vertex communication
				for params in receive_all_decrypted(conn):
					self.queue_message(*json.loads(params))
				self.buffer_count_received[addr[0]] += 1

			elif message == 'buffer_count':
				self.receive_buffer_count[addr[0]] = receive_all_decrypted(conn)


	def send_and_clear_buffer(self, rmt_host):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((rmt_host, self.worker_port))
		sock.send_all_encrypted(None)
		sock.send_all_encrypted(self.remote_message_buffer[rmt_host])
		self.remote_message_buffer[rmt_host] = []
		self.send_buffer_count[rmt_host] += 1

	# neighbor structure (vertex, edge_weight, ip)
	def vertex_send_messages_to(self, neighbor, value, superstep):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		data = (neighbor[0], value, superstep)
		if neighbor[2] != self.host:
			rmt_host = neighbor[2]
			if len(self.remote_message_buffer[rmt_host]) > self.max_buffer_size:
				self.send_and_clear_buffer(rmt_host)
			else:
				self.remote_message_buffer[rmt_host].append(data)
		else:
			self.queue_message(*data)
			self.local_global[0] += 1

		self.local_global[1] += 1


	def vertex_edge_weight(self, neighbor):
		return neighbor[1]

	def compute_each_vertex(self, superstep):
		for v in self.vertices:
			messages = self.vertex_to_messages[v]
			if self.task_id==0 and self.first_len_message[v] != len(messages) and superstep > 1:
				print 'error occurs: {},{},{}'.format(v, self.first_len_message[v], len(messages))

			vertex = self.vertices[v]

			if self.task_id==0 and not vertex.halt:
				vertex.compute(messages, superstep)

			if self.task_id==1 and (not vertex.halt or len(messages)!=0):
				vertex.compute(messages, superstep)

		for host in self.remote_message_buffer:
			self.send_and_clear_buffer(host)


	def compute(self, superstep):
		print 'Compute for Superstep {}'.format(superstep)
		start_time = time.time()
		assert(superstep == self.superstep+1)

		self.compute_each_vertex(superstep)
		for rmt_host in self.masters_workers[2:]:
			if rmt_host != self.host:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((rmt_host, self.worker_port))
				sock.send_all_encrypted('buffer_count')
				sock.send_all_encrypted(self.send_buffer_count[rmt_host])

		print 'record breakpt {} seconds'.format(time.time()-start_time)

		for rmt_host in self.masters_workers[2:]:
			if rmt_host != self.host:
				while rmt_host not in self.receive_buffer_count:
					time.sleep(1) 
				while self.receive_buffer_count[rmt_host] != self.buffer_count_received[rmt_host]:
					time.sleep(1)

		print(self.receive_buffer_count)


		self.vertex_to_messages = self.vertex_to_messages_next
		self.vertex_to_messages_next = defaultdict(list)
		self.all_halt = all(len(m)==0 for m in self.vertex_to_messages.values())
		self.superstep += 1

		assert(len(self.vertex_to_messages_next) == 0)
		print 'Compute finishes after {} seconds'.format(time.time()-start_time)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.addr, self.master_port))

		send_all_encrypted(sock, Commons.finish_compute)
		send_all_encrypted(sock, self.all_halt)

		if (self.local_global[1] == 0):
			print('No vertex processed?')
		else:
			print('local_global ratio: {}'.format(1.0*self.local_global[0]/self.local_global[1]))


from vertex import PRVertex, SPVertex
from message import send_all_encrypted
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
		self.master_port, self.worker_port = port_info
		self.task_id = task_id 
		self.key_number = key_number
		self.buffer_size = buffer_size
		self.dfs = dfs
		self.vertices = {}
		self.targetVertex = PRVertex if (task_id==0) else SPVertex

		self.masters_workers = masters_workers
		self.num_workers = len(masters_workers)-2
		self.max_vertex = None
		self.superstep = 0
		self.vertex_to_messages = defaultdict(list)
		self.vertex_to_messages_next = defaultdict(list)
		self.num_threads = 12

		# for debugging
		self.first_len_message = defaultdict(int)
		self.local_global = [0,0]

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
			print(self.vertices) 
			print(self.first_len_message)
				
	def queue_message(self, vertex, value, superstep):
		if self.superstep != superstep:
			assert(self.superstep == superstep-1)
			self.vertex_to_messages_next[vertex].append(value)
		else:
			self.vertex_to_messages[vertex].append(value)
		# should use combinator but hard to implement to be thread-safe...

	def load_to_file(self, filename):
		with open(filename, 'w') as f:
			f.write(str(self.superstep)+'\n')
			for key in sorted(self.vertices.keys()):
				v = self.vertices[key]
				f.write(str(v.vertex)+' '+str(v.value)+'\n')

	def start_main_server(self):
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.worker_port))

		while True:
			data, addr = self.monitor.recvfrom(self.buffer_size) # extra bytes are discarded
			decoded_data = json.loads(data)
			message = decoded_data.pop(0)

			if message == None:
				vertex, value, superstep = decoded_data
				self.queue_message(vertex, value, superstep)

			elif message == Commons.request_preprocess:
				start_time = time.time()
				print('receive command to load file')
				self.addr = addr[0]
				self.input_filename, self.v_to_m_dict, self.num_vertices = decoded_data
				dfsWrapper(self.dfs.getFile, self.input_filename)
				print(self.v_to_m_dict)
				self.preprocess(self.input_filename)
				print('preprocess done after {} seconds'.format(time.time()-start_time))

				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((self.addr, self.master_port))
				send_all_encrypted(sock, Commons.ack_preprocess)

			elif message == Commons.request_compute:
				superstep = decoded_data[0]
				threading.Thread(target=self.compute, args=(superstep,)).start()

			elif message == Commons.request_result: # final step
				self.output_filename = decoded_data[0]
				self.load_to_file(self.output_filename)
				dfsWrapper(self.dfs.putFile, self.output_filename)
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((self.addr, self.master_port))
				send_all_encrypted(sock, Commons.ack_result)

			elif message == Commons.end_now:
				sys.exit()
				


	#neighbor structure (vertex, edge_weight, ip)
	def vertex_send_messages_to(self, neighbor, value, superstep):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		message = json.dumps([None, neighbor[0], value, superstep])
		if neighbor[2] != self.host:
			sock.sendto(message, (neighbor[2], self.worker_port))
		else:
			self.queue_message(neighbor[0], value, superstep)

			self.local_global[0] += 1
		self.local_global[1] += 1


	def vertex_edge_weight(self, neighbor):
		return neighbor[1]

	def parallel_compute(self, superstep, start_ix, end_ix):
		for v in sorted(self.vertices.keys())[start_ix:end_ix]:
			messages = self.vertex_to_messages[v]
			if self.task_id==0 and self.first_len_message[v] != len(messages):
				print 'error occurs: {},{}'.format(self.first_len_message[v], len(messages))

			vertex = self.vertices[v]

			if self.task_id==0 and not vertex.halt:
				vertex.compute(messages, superstep)

			if self.task_id==1 and (not vertex.halt or len(messages)!=0):
				vertex.compute(messages, superstep)

			if self.task_id == 0:
				self.halt.append(vertex.halt)
			else:
				self.halt.append(len(messages)==0)


	def compute(self, superstep):
		print 'Compute for Superstep {}'.format(superstep)
		start_time = time.time()
		assert(superstep == self.superstep+1)

		self.halt = []
		threads = []

		for v in self.vertex_to_messages:
			if v not in self.vertices:
				self.vertices[v] = self.targetVertex (v, [], self.vertex_send_messages_to, self.vertex_edge_weight, self.key_number, self.num_vertices)

		for i in range(self.num_threads):
			curr_thread = threading.Thread(target=self.parallel_compute,args=(superstep, 
				i*len(self.vertices)/self.num_threads, (i+1)*len(self.vertices)/self.num_threads))
			threads.append(curr_thread)
			curr_thread.start()

		for curr_thread in threads:
			curr_thread.join()

		self.all_halt = all(self.halt)

		self.vertex_to_messages = self.vertex_to_messages_next
		self.vertex_to_messages_next = defaultdict(list)
		self.superstep += 1

		time.sleep(0.001)
		for u in self.vertex_to_messages_next.keys():
			if len(self.vertex_to_messages_next[u]) != 0:
				for m in self.vertex_to_messages_next[u]:
					self.vertex_to_messages[u].append(m)
					print 'Threads competition happens~'
				self.vertex_to_messages_next[u] = []

		print 'Compute finishes after {} seconds'.format(time.time()-start_time)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.addr, self.master_port))

		send_all_encrypted(sock, Commons.finish_compute)
		send_all_encrypted(sock, self.all_halt)

		if (self.local_global[1] == 0):
			print('No vertex processed?')
		else:
			print('local_global ratio: {}'.format(1.0*self.local_global[0]/self.local_global[1]))


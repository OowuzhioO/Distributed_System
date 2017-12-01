from vertex import PRVertex, SPVertex
from message import send_all_encrypted, receive_all_decrypted
import threading
import json 
import sys, time
import socket
from collections import OrderedDict,defaultdict
from commons import Commons, dfsWrapper, checkpt_file_name

class Worker(object):
	# host_name: hostname of machine
	# port_info: (master_port, worker_port)
	# task_id:  0 for pagerank and 1 for shortest path
	# masters_workers: master, standby, and workers in order
	# source_vertex: for shortest path
	# commons: information shared between Master and Worker
	
	def __init__(self, task_id, host_name, port_info, masters_workers, key_number, dfs, buffer_size, is_undirected):
		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.master_port, self.worker_port = port_info
		self.task_id = task_id 
		self.key_number = key_number
		self.dfs = dfs
		self.vertices = {}
		self.targetVertex = PRVertex if (task_id==0) else SPVertex

		self.masters_workers = masters_workers
		self.num_workers = len(masters_workers)-2
		self.machine_ix = self.masters_workers.index(self.host)
		self.superstep = 0
		self.vertex_to_messages = defaultdict(list)
		self.vertex_to_messages_next = defaultdict(list)
		self.vertex_to_messages_remote_next = defaultdict(list)

		self.remote_message_buffer = defaultdict(list) # key are hosts, vals are params
		self.max_buffer_size = buffer_size
		self.is_undirected = is_undirected

		# for debugging
		self.first_len_message = defaultdict(int)
		self.local_global = [0,0]

		# kept these because avoid receiving message before initialization
		self.send_buffer_count = defaultdict(int)
		self.receive_buffer_count = defaultdict(int)
		self.buffer_count_received = defaultdict(int)

	def gethost(self, vertex):
		return self.masters_workers[2+self.v_to_m_dict[vertex]]

	def init_vertex(self, u):
		if u not in self.vertices:
			self.vertices[u] = self.targetVertex (u, [],
				self.vertex_send_messages_to,self.vertex_edge_weight, self.key_number, self.num_vertices)

	def preprocess(self, filename):
		with open(filename, 'r') as input_file:
			for line in input_file.readlines():
				if line[0] < '0' or line[0] > '9':
					continue
				u, v = line.strip().split()

				if self.gethost(u) == self.host:
			   		self.init_vertex(u)
					self.vertices[u].neighbors.append([v, 1, self.gethost(v)])
					if self.is_undirected:
						self.first_len_message[u] += 1

				if self.gethost(v) == self.host:
					self.init_vertex(v)
					if self.is_undirected:
						self.vertices[v].neighbors.append([u, 1, self.gethost(u)])
					self.first_len_message[v] += 1
			self.sorted_vertices = sorted(self.vertices.keys())


		file_name = checkpt_file_name(self.machine_ix, 0)

		with open(file_name, 'w') as checkpt_f:
			for v in self.sorted_vertices:
				adj_str = str(v)+' '
				for n in self.vertices[v].neighbors:
					adj_str += str(n[0])+' '
				checkpt_f.write(adj_str+'\n')

		dfsWrapper(self.dfs.putFile, file_name)
		print('File '+file_name+' successfully save')
				

	def queue_message(self, vertex, value, superstep):
		assert(self.superstep == superstep-1)
		self.vertex_to_messages_next[vertex].append(value)
		if self.task_id == 1:
			self.vertex_to_messages_next[vertex] = [min(self.vertex_to_messages_next[vertex])]

	def queue_remote_message(self, vertex, value, superstep):
		assert(self.superstep == superstep-1)
		self.vertex_to_messages_remote_next[vertex].append(value)
		if self.task_id == 1:
			self.vertex_to_messages_remote_next[vertex] = [min(self.vertex_to_messages_remote_next[vertex])]

	def load_to_file(self, filename):
		with open(filename, 'w') as f:
			f.write(str(self.superstep)+'\n')
			for key in self.sorted_vertices:
				v = self.vertices[key]
				f.write(str(v.vertex)+' '+str(v.value)+'\n')

	def load_and_preprocess(self, conn, addr):
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

	def return_result_file(self, conn, addr):
		self.output_filename, = receive_all_decrypted(conn)
		self.load_to_file(self.output_filename)
		dfsWrapper(self.dfs.putFile, self.output_filename)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((self.addr, self.master_port))
		send_all_encrypted(sock, Commons.ack_result)

	def change_work(self, conn, addr):
		new_vertices_info, self.v_to_m_dict = receive_all_decrypted(conn) 

		for v in self.vertices:
			for neighbor in self.vertices[v].neighbors:
				if self.gethost(neighbor) != neighbor[2]:
					neighbor[2] = self.gethost(neighbor)

		for v in new_vertices_info:
			neighbors, value = new_vertices_info[v]
			self.init_vertex(v)
			for n in neighbors:
				self.vertices[v].neighbors.append([n, 1, self.gethost(n)])
			self.vertices[v].value = value

		self.sorted_vertices = sorted(self.vertices.keys())


	def start_main_server(self):
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.worker_port))
		self.monitor.listen(5)

		while True:
			try:
				conn, addr = self.monitor.accept()
				message = receive_all_decrypted(conn)

				if message == Commons.request_preprocess:
					self.load_and_preprocess(conn, addr)

				elif message == Commons.request_compute:
					superstep,checkpt = receive_all_decrypted(conn)
					self.curr_thread = threading.Thread(target=self.compute, args=(superstep,checkpt))
					self.curr_thread.daemon = True
					self.curr_thread.start()

				elif message == Commons.request_result: # final step
					self.return_result_file(conn, addr)

				elif message == Commons.end_now:
					sys.exit()

				elif message == None: # for inner vertex communication
					for params in receive_all_decrypted(conn):
						self.queue_remote_message(*params)
					self.buffer_count_received[addr[0]] += 1

				elif message == 'buffer_count':
					self.receive_buffer_count[addr[0]] = receive_all_decrypted(conn)

				else: #checkpt call
					self.curr_thread.join()

					if message == Commons.new_master:
						self.addr = addr[0]
						send_all_encrypted(conn, [self.superstep, self.all_halt])

					elif message == Commons.work_change:
						self.change_work(conn, addr)
						
			except:
				continue



	def send_and_clear_buffer(self, rmt_host):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			sock.connect((rmt_host, self.worker_port))
		except:
			pass
		send_all_encrypted(sock, None)
		send_all_encrypted(sock, self.remote_message_buffer[rmt_host])
		self.remote_message_buffer[rmt_host] = []
		self.send_buffer_count[rmt_host] += 1



	# neighbor structure (vertex, edge_weight, ip)
	def vertex_send_messages_to(self, neighbor, value, superstep):
		data = (neighbor[0], value, superstep)
		if neighbor[2] != self.host:
			rmt_host = neighbor[2]
			if len(self.remote_message_buffer[rmt_host]) > self.max_buffer_size:
				self.send_and_clear_buffer(rmt_host)
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
				sys.exit()

			vertex = self.vertices[v]

			if self.task_id==0 and not vertex.halt:
				self.vertices[v].compute(messages, superstep)

			if self.task_id==1 and (not vertex.halt or len(messages)!=0):
				self.vertices[v].compute(messages, superstep)

		for host in self.remote_message_buffer:
			self.send_and_clear_buffer(host)


	def compute(self, superstep, checkpt):
		print '\nCompute for Superstep {}'.format(superstep)
		start_time = time.time()
		assert(superstep == self.superstep+1)

		self.compute_each_vertex(superstep)
		try:
			for rmt_host in self.masters_workers[2:]:
				if rmt_host != self.host:
					sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					sock.connect((rmt_host, self.worker_port))
					send_all_encrypted(sock, 'buffer_count')
					send_all_encrypted(sock, self.send_buffer_count[rmt_host])
		except:
			pass #forfeit the current compute

		print 'record breakpt {} seconds'.format(time.time()-start_time)

		for rmt_host in self.masters_workers[2:]:
			if rmt_host != self.host:
				while rmt_host not in self.receive_buffer_count:
					time.sleep(1) 
				while self.receive_buffer_count[rmt_host] != self.buffer_count_received[rmt_host]:
					time.sleep(1)

		self.send_buffer_count = defaultdict(int)
		self.receive_buffer_count = defaultdict(int)
		self.buffer_count_received = defaultdict(int)

		self.vertex_to_messages = defaultdict(list)
		for v in self.vertices:
			self.vertex_to_messages[v] = self.vertex_to_messages_next[v]+self.vertex_to_messages_remote_next[v]

		self.vertex_to_messages_next = defaultdict(list)
		self.vertex_to_messages_remote_next = defaultdict(list)
		self.all_halt = all(len(m)==0 for m in self.vertex_to_messages.values())
		self.superstep += 1

		if checkpt:
			file_name = checkpt_file_name(self.machine_ix, superstep)
			self.load_to_file(file_name)
			dfsWrapper(self.dfs.putFile, file_name)
			print('File '+file_name+' successfully save')

		assert(len(self.vertex_to_messages_next) == 0)
		print 'Compute finishes after {} seconds'.format(time.time()-start_time)

		if (self.local_global[1] == 0):
			print('No vertex processed')
		else:
			print('local_global ratio: {}'.format(1.0*self.local_global[0]/self.local_global[1]))

		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((self.addr, self.master_port))
			send_all_encrypted(sock, Commons.finish_compute)
			send_all_encrypted(sock, self.all_halt)
		except:
			pass

		


from vertex import PRVertex, SPVertex
from message import send_all_encrypted
import threading
import json 
import sys, time

class Worker(object):
	# host_name: hostname of machine
	# port_info: (master_port, worker_port)
	# task_id:  0 for pagerank and 1 for shortest path
	# masters_workers: master, standby, and workers in order
	# source_vertex: for shortest path
	# commons: information shared between Master and Worker
	
	def __init__(self, task_id, host_name, port_info, masters_workers, source_vertex, commons):
		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.master_port, self.worker_port = port_info
		self.task_id = task_id 
		self.source = source_vertex
		self.buffer_size = 40

		self.split_filename, self.ack_preprocess, self.request_compute, 
		self.finish_compute, self.request_result, self.ack_result = commons
		self.dfs = dfs
		self.vertices = {}
		self.targetVertex = PRVertex if (task_id==0) else SPVertex

		self.masters_workers = masters_workers
		self.num_workers = len(masters_workers)-2
		self.max_vertex = None
		self.superstep = 0
		self.vertex_to_messages = {v:[] for v in vertices.keys()}
		self.vertex_to_messages_next = {v:[] for v in vertices.keys()}
		self.num_threads = 12

		# for debugging
		self.first_len_message = {}
		self.local_global = (0,0)


	def preprocess(self, filename):
		with open(filename, 'w') as input_file:
			for line in input_file.readlines():
				if line[0] == '#' or line[0] == '/':
					continue
				u, v = line.split()
				u, v = int(u), int(v)
				if u not in self.vertices:
					neighbor_host = self.masters_workers[2+min(v*self.num_workers/self.max_vertex, self.num_workers-1)]
					self.vertices[u] = self.targetVertex (u, [v, 1, neighbor_host]
						u==self.source_vertex, self.vertex_send_messages_to,self.vertex_edge_weight)
				else:
					self.vertices[u].neighbors.append(v)
			
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
			decoded_data = json.decode(data)
			message = decoded_data.pop(0)

			if message == None:
				vertex, value, superstep = decoded_data
				self.queue_message(vertex, value, superstep)

			elif message.startswith(self.split_filename):
				self.filename = message
				self.max_vertex = decoded_data[0]
				self.dfs.getFile(self.filename)
				self.preprocess(self.filaname)

				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((addr, self.master_port))
				send_all_encrypted(self.ack_preprocess)

			elif message == self.request_compute:
				superstep = decoded_data[0]
				threading.Thread(self.compute, args=(superstep,)).start()

			elif message == self.request_result: # final step
				self.load_to_file(self.filename)
				self.dfs.putFile(self.filename)
				
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((addr, self.master_port))
				send_all_encrypted(self.ack_result)
				sys.exit()
				


	#neighbor structure (vertex, edge_weight, ip)
	def vertex_send_messages_to(self, neighbor, value, superstep):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		message = json.dumps([None, neighbor[0], value, superstep])
		if neighbors[2] != self.host:
			sock.sendto(message, (neighbor[2], self.worker_port))
		else:
			self.queue_message(neighbors[0], value, superstep)

			self.local_global[0] += 1
		self.local_global[1] += 1


	def vertex_edge_weight(self, neighbor):
		return neighbor[1]

	def parallel_compute(self, superstep, start_ix, end_ix):
		for v in self.vertices[start_ix:end_ix]:
			if superstep == 1:
				self.first_len_message[v] = len(self.vertex_to_messages[v])
			else:
				if self.task_id==0 and self.first_len_message[v] != len(self.vertex_to_messages[v]):
					print 'error occurs: {},{}'.format(self.first_len_message[v], len(self.vertex_to_messages[v]))

			if self.task_id==0 and not v.halt:
				v.compute(self.vertex_to_messages[v], superstep)

			if self.task_id==1 and (not v.halt or len(self.vertex_to_messages[v])!=0):
				v.compute(self.vertex_to_messages[v], superstep)

			self.halt.append(v.halt)


	def compute(self, superstep):
		start_time = time.time()
		assert(superstep == self.superstep+1)

		self.halt = []
		for i in range(self.num_threads):
			curr_thread = threading.Thread(target=self.parallel_compute,args=(superstep, 
				i*len(self.vertices)/num_threads, (i+1)*len(self.vertices)/num_threads))
			threads.append(curr_thread)
			curr_thread.start()

		for curr_thread in threads:
			curr_thread.join()

		self.all_halt = all(self.halt)

		self.vertex_to_messages = vertex_to_messages_next
		self.vertex_to_messages_next = {v:[] for v in vertices.keys()}
		self.superstep += 1

		time.sleep(0.001)
		for u in self.vertex_to_messages_next.keys():
			if len(self.vertex_to_messages_next[u]) != 0:
				for m in self.vertex_to_messages_next[u]:
					self.vertex_to_messages[u].append(m)
					print('Threads competition happens~')
				self.vertex_to_messages_next[u] = []

		print 'last message sent after {} seconds'.format(time.time()-start_time)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((addr, self.master_port))

		send_all_encrypted(self.finish_compute)
		send_all_encrypted(self.all_halt)
		print('local_global ratio: {}'.format(1.0*self.local_global[0]/self.local_global[1]))


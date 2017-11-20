from vertex import PRVertex, SPVertex
from message import send_all_encrypted, send_all_from_file
import json 
import time

class Worker(object):
	# hostName: hostname of machine
	# port_num: port num designed for graph processing
	# vertices: names of vertices in the partition
	# out_edges: adjacency list of vertices
	# dict_vertex_to_machine: vertex to machine dict 
	# super_step_interval: time designed for each superstep
	# task_id:  0 for pagerank and otherwise shortest path
	# source_vertex: source in shortest path (can be anything in other applications)
	def __init__(self, host, port, task_id, commons, getParams, masters_workers, source_vertex=None):
		self.host = host
		self.port = port
		self.task_id = task_id 
		self.source = source_vertex
		self.buffer_size = 32
		self.split_filename, self.ack_preprocess = commons
		self.dfs = dfs
		self.getParams = getParams
		self.vertices = {}
		self.targetVertex = PRVertex if (task_id==0) else SPVertex
		self.masters_workers = masters_workers
		self.num_workers = len(masters_workers)-2
		self.max_vertex = max_vertex

	def preprocess(self, filename):
		with open(filename, 'w') as input_file:
			for line in input_file.readlines():
				if line[0] == '#' or line[0] == '/':
					continue
				u, v = line.split()
				u, v = int(u), int(v)
				if u not in self.vertices:
					self.vertices[u] = self.targetVertex (u, [v, 1, 
						socket.gethostbyname(self.masters_workers[2+min(v*num_workers/max_vertex, num_workers-1)])], 
						u==source_vertex, self.vertex_send_messages_to, 
						self.vertex_vote_to_halt, self.vertex_edge_weight)
				else:
					self.vertices[u].neighbors.append(v)
			

	def background_server(self):
		self.vertex_to_messages = {}
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.port))

		while True:
			data, addr = self.monitor.recvfrom(self.buffer_size) # extra bytes are discarded

			if data.startswith(self.split_filename):
				self.dfs.getFile(filename)
				self.preprocess(data)

				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((addr, self.port))
				send_all_encrypted(self.ack_preprocess)
				continue

			vertex, value = json.loads(data)

			if vertex not in vertex_to_messages:
				vertex_to_messages[vertex] = []
			vertex_to_messages[vertex].append(value)

			if task_id != 0:	# use combinator
				vertex_to_messages[vertex] = [min(vertex_to_messages[vertex])] 

			time.sleep(ss_interval)


	def vertex_vote_to_halt(self):
		pass

	#neighbor structure (vertex, edge_weight, ip)
	def vertex_send_messages_to(self, neighbor, value):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		message = json.dumps((neighbor[0], value))
		sock.sendto(message, (neighbor[2], self.port))

	def vertex_edge_weight(self, neighbor):
		return neighbor[1]

	def work(self):
		while not all_halt(): #TODO
			for v in self.vertices:
				v.compute()
					
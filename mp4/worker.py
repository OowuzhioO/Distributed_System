from vertex import PRVertex, SPVertex
import json 
import time

class worker(object):
	# hostName: hostname of machine
	# port_num: port num designed for graph processing
	# vertices: names of vertices in the partition
	# out_edges: adjacency list of vertices
	# dict_vertex_to_machine: vertex to machine dict 
	# super_step_interval: time designed for each superstep
	# task_id:  0 for pagerank and otherwise shortest path
	# source_vertex: source in shortest path (can be anything in other applications)
	def __init__(self, host, port, vertices, out_edges, dict_vertex_to_machine, super_step_interval, task_id, source_vertex=None):
		self.host = host
		self.port = port
		self.temp_vertices = vertices
		self.out_edges = out_edges
		self.dict_vm = dict_vertex_to_machine
		self.ss_interval = super_step_interval
		self.task_id = task_id 
		self.source = source_vertex
		self.buffer_size = 32

	def background_server(self):
		self.vertex_to_messages = {}
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.port))

		while True:
			data, addr = self.monitor.recvfrom(self.buffer_size) # extra bytes are discarded
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
		targetVertex = PRVertex if (task_id==0) else SPVertex
		self.vertices = [targetVertex(out_edges[v], v==self.source, self.vertex_send_messages_to, 
						self.vertex_vote_to_halt, self.vertex_edge_weight) for v in self.temp_vertices]
		while not all_halt(): #TODO
			for v in self.vertices:
				v.compute()
					
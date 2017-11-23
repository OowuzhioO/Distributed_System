class Vertex:
	# send_messages_to(neighbor, value): sends value to neighbor
	# edge_weight(neighbor):  returns weight of the outgoing edge to the neighbor
	def __init__(self, vertex, neighbors, send_messages_to, edge_weight, key_number, num_vertices):
		self.vertex = vertex
		self.value = float('inf')
		self.neighbors = neighbors
		self.send_messages_to = send_messages_to
		self.halt = False
		self.edge_weight = edge_weight
		self.is_source = key_number == self.vertex
		self.num_iterations = key_number
		self.num_vertices = num_vertices
		# ...

	def vote_to_halt(self):
		self.halt = True

	def send_to_all_neighbors(self, value, super_step):
		for neighbor in self.neighbors:
			self.send_messages_to(neighbor, value, super_step)

# Page rank Vertex 
class PRVertex(Vertex):

	# compute at each super_step
	# messages are basically values
	def compute(self, messages, super_step):
		self.halt = False
		if super_step > 0:
			self.value = 0.15/self.num_vertices+0.85*sum(messages)

		if (super_step < self.num_iterations) and len(self.neighbors) != 0:
			self.send_to_all_neighbors(self.value/len(self.neighbors), super_step)
		else:
			self.vote_to_halt()

# Shortest path Vertex, same function declaration as PRVertex
class SPVertex(Vertex):
		
	def compute(self, messages, super_step):
		self.halt = False
		min_dist = 0 if self.is_source else float('inf')
		for m in messages:
			min_dist = min(min_dist, m) 

		if (min_dist < self.value):
			self.value = min_dist
			for neighbor in self.neighbors:
				update_val = self.edge_weight(neighbor)+min_dist
				self.send_messages_to(neighbor, update_val, super_step)
				
		self.vote_to_halt() 	

			

	

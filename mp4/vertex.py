class Vertex:
	# is_source: True iff the vertex is the source of path
	# send_messages_to(neighbor, value): sends value to neighbor
	# vote_to_halt(): vote to halt 
	# edge_weight(neighbor):  returns weight of the outgoing edge to the neighbor
	def __init__(self, vertex, neighbors, is_source, send_messages_to, edge_weight):
		self.vertex = vertex
		self.value = None
		self.neighbors = neighbors
		self.is_source = is_source
		self.send_messages_to = send_messages_to
		self.halt = False
		self.edge_weight = edge_weight
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
			self.value = 0.15/len(messages)+0.85*sum(messages)

		if super_step < 20:
			self.send_to_all_neighbors(self.value/len(self.neighbors), super_step)
		else:
			self.vote_to_halt()

# Shortest path Vertex, same function declaration as PRVertex
class SPVertex(Vertex):
		
	def compute(self, messages, super_step):
		self.halt = False
		if not self.is_source:
			min_dist = min(messages) if self.value==None \
						else min(min(messages), self.value)

			if (min_dist < self.value):
				self.value = min_dist
				for neighbor in neighbors:
					update_val = self.edge_weight(neighbor)+min_dist
					self.send_messages_to(neighbor, update_val, super_step)
			else:
				self.vote_to_halt()

			

	
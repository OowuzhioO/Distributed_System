class Vertex:
	def __init__(self, neighbors, is_source, send_messages_to, vote_to_halt, edge_weight):
		self.super_step = 0
		self.value = None
		self.neighbors = neighbors
		self.is_source = is_source
		self.send_messages_to = send_messages_to
		self.vote_to_halt = vote_to_halt
		self.edge_weight = edge_weight
		# ...

	def send_to_all_neighbors(self, value):
		for neighbor in self.neighbors:
			self.send_messages_to(neighbor, value)

# Page rank Vertex 
class PRVertex(Vertex):

	# compute at each super_step
	# messages are basically values
	def compute(self, messages):
		if self.super_step > 0:
			self.value = 0.15/len(messages)+0.85*sum(messages)

		if self.super_step < 30:
			self.send_to_all_neighbors(self.value/len(self.neighbors))
		else:
			self.vote_to_halt()

		self.super_step += 1

# Shortest path Vertex, same function declaration as PRVertex
class SPVertex(Vertex):
		
	def compute(self, messages):
		if not self.is_source:
			min_dist = min(messages) if self.value==None \
						else min(min(messages), self.value)
			if (min_dist < self.value):
				self.value = min_dist
				for neighbor in neighbors:
					self.send_messages_to(neighbor, self.edge_weight(neighbor)+min_dist)
			else:
				self.vote_to_halt()

			self.super_step += 1

			

	
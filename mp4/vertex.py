class Vertex:
	def __init__(self, neighbors, edge_weights, is_source):
		self.super_step = 0
		self.value = None
		self.neighbors = neighbors
		self.edge_weights = edge_weights
		self.is_source = is_source
		# ...

	def edge_weight(neighbor):
		pass

	def send_messages_to(neighbor, value):
		pass

	def send_to_all_neighbors(value):
		pass 

	def vote_to_halt():
		pass

# Page rank Vertex 
class PRVertex(Vertex):
	def can_combine():
		return False # check first to see if combine is possible

	# compute at each super_step
	# messages are basically values
	def compute(self, messages):
		if self.super_step > 0:
			self.value = 0.15/len(messages)+0.85*sum(messages)

		if self.super_step < 30:
			send_to_all_neighbors(self.value/len(self.neighbors))
		else:
			vote_to_halt()

		self.super_step += 1

# Shortest path Vertex, same function declaration as PRVertex
class SPVertex(Vertex):
	def can_combine():
		return True # check first to see if combine is possible

	# combine multiple messages into 1
	def combine(self, messages):
		return min(messages)
		
	def compute(self, messages):
		if not self.is_source:
			min_dist = min(messages) if self.value==None \
						else min(min(messages), self.value)
			if (min_dist < self.value):
				self.value = min_dist
				for neighbor in neighbors:
					send_messages_to(neighbor, edge_weight(neighbor)+min_dist)
			else:
				vote_to_halt()

			self.super_step += 1

			

	
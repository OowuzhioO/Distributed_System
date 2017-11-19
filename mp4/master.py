import random
class master:
	def __init__(self, memblist, all_vertices, all_edges, task_id):
		self.memblist = memblist
		self.all_vertices = all_vertices
		self.all_edges = all_edges
		self.adjlist = {v:{} for v in all_vertices}
		for v,target in all_edges:
			self.adjlist[v].append(target)
		self.task_id = task_id

	def rearrange_vertices():
		random.shuffle(self.all_vertices)
		return all_vertices

	def partition(all_vertices, all_edges, num_workers):
		all_vertices = rearrange_vertices()
		piece_len = 1.0*len(all_vertices)/num_workers
		for i in range(num_workers):
			target_vertices = all_vertices[i, (i+piece_len 
							if i != num_workers-1 else len(target_vertices)-1)]
			target_neighbors = {v:self.adjlist[v] for v in target_vertices}

	def start_working(task_id):
		pass

	def halt_server():
		pass

	
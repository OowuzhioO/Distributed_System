import sys

def get_vertex(line):
	return int(line.split()[0])

def parse_file(graph_filename, num_machines):
	with open(graph_filename, 'r') as graph_file:
		lines = graph_file.readlines()
		v_to_m_dict = {}
		curr_counter = 0

		for line in lines:
			if line[0] < '0' or line[0] > '9':
					continue
			u, v = line.split()
			if u not in v_to_m_dict:
				v_to_m_dict[u] = curr_counter
				curr_counter += 1
			if v not in v_to_m_dict:
				v_to_m_dict[v] = curr_counter
				curr_counter += 1

		num_vertices = curr_counter
		v_to_m_dict = {v: num_machines*i/num_vertices for v,i in v_to_m_dict.items()}
	
	return v_to_m_dict, num_vertices
	
# process vertices results into 1
def combine_files(output_filename, collected_files):
	supersteps = []
	unsorted_pairs = []
	for collected_file in collected_files:
		with open(collected_file, 'r') as input_file:
			lines = input_file.readlines()
			supersteps.append(int(lines[0]))
			for line in lines[1:]:
				x, y = line.split()
				unsorted_pairs.append((int(x), float(y)))
		
	with open(output_filename, 'w') as output_file:
		for x,y in sorted(unsorted_pairs):
			output_file.write('{} {}\n'.format(x,y))

	assert(len(set(supersteps)) <= 1)



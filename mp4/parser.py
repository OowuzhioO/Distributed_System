import sys

def get_vertex(line):
	return int(line.split()[0])

def binary_search(lines, target):
	low, high = 0, len(lines)-1	
	while (low < high):
		mid = (low+high)/2
		result = get_vertex(lines[mid])
		if (result == target):
			break
		elif (result < target): 
			low = mid+1
		else:
		 	high = mid-1

	while low < len(lines) and get_vertex(lines[low]) <= target: 
		low = low+1
	while low >= len(lines) or (low > 0 and get_vertex(lines[low]) > target):
		low = low-1
	return low

# split files based on uniform partition on vertices
def split_files(graph_filename, output_files):
	with open(graph_filename, 'r') as graph_file:
		lines = graph_file.readlines()

		num_pieces = len(output_files)
		max_vertex = get_vertex(lines[-1])
		files = [open(f,'w') for f in output_files]

		end_ix = -1
		for file_ix in range(num_pieces): 
			start_ix = end_ix+1
			target = (file_ix+1)*max_vertex/num_pieces
			end_ix = binary_search(lines, target)
			for line in lines[start_ix:end_ix+1]:
				files[file_ix].write(line)
			
		num_vertices = max_vertex
		for line in lines:
			if line[0] >= '0' and line[0] <= '9':
				break
			targets = 'ode:','ertices:', 'ode :', 'ertices :'
			for target in targets:
				if target in line: 
						num_vertices = int(line[line.index(target)+len(target):].split()[0])
						break

		for f in files:
			f.close()
	
	return max_vertex, num_vertices
	
# process vertices results into 1
def combine_files(output_filename, collected_files):
	supersteps = []
	should_be_sorted = []
	with open(output_filename, 'w') as output_file:
		for collected_file in collected_files:
			with open(collected_file, 'r') as input_file:
				lines = input_file.readlines()
				supersteps.append(int(lines[0]))
				should_be_sorted.append(int(lines[1].split()[0]))
				should_be_sorted.append(int(lines[-1].split()[0]))

			for line in lines[1:]:
				output_file.write(line)

	assert(len(set(supersteps)) <= 1)
	assert(sorted(should_be_sorted)==should_be_sorted)

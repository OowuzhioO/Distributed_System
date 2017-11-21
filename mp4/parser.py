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
	with open(sys.argv[1]) as graph_file:
		lines = graph_file.readlines()
		num_pieces = len(output_files)
		num_vertices = get_vertex(lines[-1])
		files = [open(output,'w') for f in output_files]

		end_ix = -1
		for file_ix in range(num_pieces): 
			start_ix = end_ix+1
			target = (file_ix+1)*num_vertices/num_pieces
			end_ix = binary_search(lines, target)
			for line in lines[start_ix:end_ix+1]:
				files[file_ix].write(line)
			
		for f in files:
			f.close()
		
		

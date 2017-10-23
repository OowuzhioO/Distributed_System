import sys

# Use hash on an ring of all ints
def left_dist(curr, target):
    return (hash(curr)-hash(target)+sys.maxint*2+1)%(sys.maxint*2+1) # over all possible range 

# simple binary search to find insertion index
def binary_search(mlist, curr, target):
	if (len(mlist) == 0):
		return 0
	low, high = 0, len(mlist)-1
	while (low <= high):
		mid = (low+high)/2
		if mlist[mid] == target:
			return mid
		if left_dist(curr, mlist[mid]) < left_dist(curr, target):
			low = mid+1
		else:
			high = mid-1
	return low if left_dist(curr, mlist[mid]) < left_dist(curr, target) else high+1

def update_neighbors(target_id, sorted_memlist, curr_id, curr_neighbors):
	i = binary_search(sorted_memlist, curr_id, target_id)
	if i < len(sorted_memlist) and sorted_memlist[i] == target_id:#target_id in sorted_memlist:
		# delete it and find next best
		if target_id in curr_neighbors:
			curr_neighbors.remove(target_id)
			if len(curr_neighbors) == 3:
				if i < 2:
					curr_neighbors.append(sorted_memlist[2])
				else:
					curr_neighbors.append(sorted_memlist[-3])
		sorted_memlist.remove(target_id)
	else: 
		# new element, use binary search result to find next
		sorted_memlist.insert(i, target_id)
		if i < 2:
			if len(curr_neighbors) == 4:
				curr_neighbors.remove(sorted_memlist[2])
			curr_neighbors.append(target_id)
		elif i > len(sorted_memlist)-3:
			if len(curr_neighbors) == 4:
				curr_neighbors.remove(sorted_memlist[len(sorted_memlist)-3])
			curr_neighbors.append(target_id)	

import hashlib

ring_size = 256
def get_ring_pos(string):
	m = hashlib.sha1()
	m.update(string)
	return int(m.hexdigest(),16)%ring_size

def get_ring_pos_of_machine(machine_id, port):
	return get_ring_pos(machine_id + port, ring_size)

def get_file_location(file_name, machine_positions):
	# may require changing machine_positions structure
	file_pos = get_ring_pos(file_name, ring_size)

	chosen_machine, min_val = None, ring_size
	for (ix,pos) in machine_positions:
		dist = (pos-file_pos+ring_size) % ring_size
		if (dist < min_val):
			chosen_machine, min_val = ix, dist
	return chosen_machine

def get_replica_group(file_name, member_list):
	pass

def get_quorum(replica_group):
	pass

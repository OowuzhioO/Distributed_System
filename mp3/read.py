
def read_file(file_name, member_list): # can be changed later......
	group = get_replica_group(file_name, member_list)
	machine_i = latest_time_stamp(file_name, group)
	read_from_machine(file_name, machine_i)
	
# figure out the machine number with latest time stamp in the quorum
def latest_time_stamp(file_name, group):	
	pass

def read_from_machine(file_name, machine_i):
	pass

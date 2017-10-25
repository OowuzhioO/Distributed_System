
def write_file(file_name, member_list): # can be changed later......
	group = get_replica_group(file_name, member_list)
	if not check_write_write_conflict(file_name, group):
		quorum = get_quorum(group)
		write_to_quorum(file_name, quorum)	

def delete_file(file_name, member_list): # can be changed later......
	group = get_replica_group(file_name, member_list)
	#TODO: delete every file in the group

											  
# handle write_write_conflict, return False if No conflict	
def check_write_write_conflict(file_name, group): 
	pass

# write to some, and leave other in backgroup
def write_to_quorum(file_name, quorum)
	pass


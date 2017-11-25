from time import sleep
import traceback

class Commons:
	request_preprocess = 'Preprocess for me'
	ack_preprocess = 'Preprocess finished'
	request_compute = 'Please Compute'
	finish_compute = 'Compute Done'
	request_result = 'Result?'
	ack_result = 'Here are the results'
	end_now = 'End now before it\'s too late'

def dfsWrapper(dfs_opt, filename):
	try:
		dfs_opt(filename)
	except Exception as e:
		traceback.print_exc()
		sleep(1)
		dfsWrapper(dfs_opt, filename)

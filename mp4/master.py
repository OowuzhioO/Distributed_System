import random
from parser import split_files
from threading import Thread
from time import sleep
from message import receive_all_decrypted, receive_all_to_target

class Master:
	def __init__(self, memblist, task_id, filename, masters_workers, host, master_port, worker_port, port, dfs, commons):
		self.memblist = memblist
		self.task_id = task_id
		self.filename = filename
		self.masters_workers = masters_workers
		self.num_workers = len(masters_workers)-2
		self.split_filename, self.ack_preprocess = commons

		self.host_name = host_name
		self.host = socket.gethostbyname(host_name)
		self.master_port = master_port
		self.worker_port = worker_port
		self.driver_port = port
		self.dfs = dfs

		self.num_preprocess_done = 0

	def background_server(self):
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.server_sock.bind((self.host, self.port))
		self.server_sock.listen(5)

		while True:
			conn, addr = self.server_sock.accept()				
			rmtHost= socket.gethostbyaddr(addr[0])[0]
			if receive_all_decrypted(conn) == 'Preprocess Done':
				self.num_preprocess_done += 1

	def wait_for_ack(self):
		while (self.num_preprocess_done < num_workers):
			sleep(1)

	def start_working():
		self.server_task = Thread(target=self.background_server)
		self.server_task.daemon = True
		self.server_task.start()

		output_files = [self.split_filename+str(i+1) for i in range(num_workers)]
		split_files(self.filename, output_files)

		for ix in range(len(output_files)):
			# put file
			self.dfs.putFile(output_files[ix])
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.sendto(output_files[ix], (self.masters_workers[ix+2], self.worker_port))
		self.wait_for_ack()


	def halt_server():
		pass

	
from collections import OrderedDict,defaultdict
from message import send_all_encrypted, send_all_from_file, receive_all_decrypted, receive_all_to_target
import socket
import threading
import subprocess
from time import localtime, strftime
import time
import logging

import os, sys
import os.path as osp
import argparse
import struct

import json # for serialize heatbeat messages
import pdb
import pprint
import datetime
import random


VM_DICT={} # simple convert vm name to simple names
VM_DICT.update({socket.gethostname():'localhost'})

# total order figure out at the end
# send start time is the key


def stampedMsg(msg):
	return strftime("[%Y-%m-%d %H:%M:%S] ", localtime())+str(msg)


### customized timer function ###
class Timer(object):
    """A simple timer."""
    def __init__(self):
        self.total_time = 0.
        self.calls = 0
        self.start_time = 0.
        self.diff = 0.
        self.average_time = 0.

    def tic(self):
        # using time.time instead of time.clock because time time.clock
        # does not normalize for multithreading
        self.start_time = time.time()

    def toc(self, average=False):
        self.diff = time.time() - self.start_time
        self.total_time += self.diff
        self.calls += 1
        self.average_time = self.total_time / self.calls
        if average:
            return self.average_time
        else:
            return round(self.diff,8)



### main object ###
class distributed_file_system(object):
	#  added membList 
	def __init__(self, hostName, groupID, VM_DICT, membList, w_quorum =3, r_quorum = 2):
		## input hostName -- this node's group id after joining
		## VM_DICT -- mapping host name to node name

		self.hostName=hostName
		self.VM_DICT = VM_DICT
		self.VM_INV = {v:k for k,v in VM_DICT.items()} # inverse dict of VM_DICT
		self.nodeName = self.VM_DICT[self.hostName]
		self.host = socket.gethostbyname(self.hostName)
		self.port = 5363 
		# membership list, passed in reference so can know the current members even within the class
		# However can't change it and should not use it to check churn
		# Instead each churn should call the corresponding function of this class
		self.membList = membList

		# a list of information about file
		self.global_file_info = {} # each element is filename: [latest update time, list of nodes storing the file]
		self.local_file_info = {} # each element is  filename: [timestamp when receiving this file]
		self.timer = Timer() # use custimized timer instead of time.time

		self.w_quorum = w_quorum
		self.r_quorum = r_quorum

		# initlize group id, later to be changed by self.joinGrp()
		self.groupID = groupID
		# token for introduction and leave
		self.message_file = 'The following is for file content'
		self.message_data = 'Following is information of new file'
		self.message_ask_time = 'Please give me the last update time for the following file'
		self.message_ask_file = 'Please send me the cur file content for the following file'
		self.message_delete_data = 'Please delete the infomration of this file'
		self.message_delete_file = 'Please delete the current content of this file'

		monitor = threading.Thread(target=self.server_task)
		monitor.daemon=True
		monitor.start()

	# Ideal design: only server is allowed to change file or meta-data information 
	# Though should not for example start a new connection (deadlock)
	def server_task(self):
		#first, start local timer, the rest of the process follows this timer
		self.timer.tic()


		# a monitor receive message, check and response, also multicase failure message
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.port))
		self.monitor.listen(5)


		# self.monitor.listen(10) # UDP doesn't support this
		logging.info(stampedMsg('FS Monitoring process opens.'))
		
		# keep receiving msgs from other VMs
		# receiving heartbeat and other messages 

		# pdb.set_trace()
		while True:
			try:
				conn, addr = self.monitor.accept()
				rmtHost= socket.gethostbyaddr(addr[0])[0]
				logging.debug(stampedMsg('FS Monitor recieve instruction from {}').format(rmtHost))
			
			except socket.error, e:
				logging.warning("Caught exception socket.error : %s" %e)
				logging.warning(stampedMsg('Fail to receive signal from clients {}'.format(rmtHost)))
				break #TODO: should we break listening if UDP reception has troubles?

			message = receive_all_decrypted(conn) # the instruction
			if not message: # possibly never called in UDP					
				logging.info(stampedMsg('Receiving stop signal from clients {}'.format(rmtHost)))
				break

			# log whatever recieved
			logging.debug(stampedMsg(message))

			if message == self.message_file: # if receive leave signal
				# include filename info for debugging purposes
				if rmtHost == self.hostName:
					filename = str(receive_all_decrypted(conn))
				else:
					filename = receive_all_to_target(conn)
				logging.info(stampedMsg('receiving file {} from {}'.format(filename, rmtHost)))
				self.local_file_info[filename] = datetime.datetime.now().isoformat()

			elif message == self.message_data: # ....
				filename, file_nodes = receive_all_decrypted(conn)
				filename = str(filename) # get rid of annoying utf-encoding prefix
				file_nodes = list(map(str, file_nodes))
				self.global_file_info[filename] = (self.timer.toc(), file_nodes) # time should sent over instead of local

			elif message == self.message_ask_time:
				filename = receive_all_decrypted(conn)
				send_all_encrypted(conn, self.local_file_info[filename])

			elif message == self.message_ask_file:
				filename = receive_all_decrypted(conn)
				send_all_from_file(conn, filename)

			elif message == self.message_delete_data:
				filename = receive_all_decrypted(conn)
				if filename in self.global_file_info:
					del self.global_file_info[filename]

			elif message == self.message_delete_file:
				filename = receive_all_decrypted(conn)
				if filename in self.local_file_info:
					del self.local_file_info[filename]
					# Let's leave the real file there for now .....

		return None 

	# Below are 3 main function for accessing/modifying DFS: put/get/delete

	def putFile(self, filename):
		# failed should be passed as a groupID
		# use should use a temporary file for consistency (e.g. error in middle of transmission)
		if (filename in self.global_file_info):
			# broadcast to that group
			target_processes = [node for node in self.global_file_info[filename][-1] if node != self.groupID]
			self.broadCastFile(target_processes, filename)
			# simple synchronization assumption

		else:
			target_processes = random.sample(self.membList.keys(), min(self.w_quorum, len(self.membList)))
			if self.groupID not in target_processes:
				target_processes = target_processes[1:]+[self.groupID]	

			self.broadCastFile(target_processes, filename) 
			self.broadCastData(self.membList.keys(), (filename, target_processes))


	def getFile(self, filename):
		# check metadata first
		if (filename in self.global_file_info):
			replicas_nodes = self.global_file_info[filename][-1]
			target_nodes = random.sample(replicas_nodes, min(self.r_quorum, len(replicas_nodes)))
			target = self.mostRecentNode(target_nodes, filename)
			self.askForFile(target, filename)
			return True 
		else:
			return False


	def deleteFile(self, filename):
		if (filename in self.global_file_info):
			replicas_nodes = self.global_file_info[filename][-1]
			self.broadCastData_delete(self.membList.keys(), filename)
			self.broadCastFile_delete(replicas_nodes, filename)
			return True 
		else:
			return False


	def replicate(self, failed_process, left_over_replicas, filename):
		no_replica = [node for node in self.membList.keys() \
			if (node not in left_over_replicas) and node != failed_process]
		next_replica = random.sample(no_replica, min(1, len(no_replica))) # empty list or size 1
		try:
			self.broadCastFile(next_replica, filename)
			self.broadCastData(self.membList.keys(), (filename, next_replica+left_over_replicas))
		except: # 2 simultaneous fail
			no_replica = [node for node in no_replica if node != next_replica]
			next_replica = random.sample(no_replica, min(1, len(no_replica)))
			self.broadCastFile(next_replica, filename)
			self.broadCastData(self.membList.keys(), (filename, next_replica+left_over_replicas))


	# Need to be called for replication of metadata on time
	# should be called after memList is updated
	def onProcessFail(self, failed_process):
		# do re-replication
		logging.info(stampedMsg('Process {} failed, re-replicate files'.format(failed_process)))
		for file, infos in self.global_file_info.items():
			replicas = infos[-1]
			if failed_process in replicas:
				replicas.remove(failed_process)
				if len(replicas) > 0 and self.groupID == replicas[0]:
					self.replicate(failed_process, replicas, file)



	# helper function to reduce code redundancy/duplication
	def getParams(self, target):
		target_hostname = target.split('_')[0]
		target_host = socket.gethostbyname(target_hostname)
		target_nodeName = self.VM_DICT[target_hostname]
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((target_host, self.port)) # call might fail
		return target_host, target_nodeName, sock 


	# target should be in membershiplist key format (groupID)
	def broadCastData(self, targets, data):
		for target in targets:
			target_host, target_nodeName, sock = self.getParams(target)
			send_all_encrypted(sock, self.message_data)
			send_all_encrypted(sock, data)

		logging.debug(stampedMsg('broadCast Data: {}'.format(data)))

	# target should be in membershiplist key format (groupID)
	def broadCastFile(self, targets, filename):
		for target in targets:
			target_host, target_nodeName, sock = self.getParams(target)
			send_all_encrypted(sock, self.message_file)
			if target == self.groupID:
				send_all_encrypted(sock, filename)
			else:
				send_all_from_file(sock, filename)
			logging.debug(stampedMsg('{} pushing file {} to node {}'.format(self.nodeName, filename, target_nodeName)))

	# return the most recent node for polling result
	def mostRecentNode(self, targets, filename):
		max_time, max_target = None, None
		for target in targets:
			target_host, target_nodeName, sock = self.getParams(target)
			send_all_encrypted(sock, self.message_ask_time)
			send_all_encrypted(sock, filename)
			timestamp = receive_all_decrypted(sock)
			if max_time == None or timestamp > max_time:
				max_time, max_target = timestamp, target
		return max_target


	def askForFile(self, target, filename):
		if target == self.groupID:
			return
		target_host, target_nodeName, sock = self.getParams(target)
		send_all_encrypted(sock, self.message_ask_file)
		send_all_encrypted(sock, filename)
		receive_all_to_target(sock)


	def broadCastData_delete(self, targets, data): # in this case, data is just filename
		for target in targets:
			target_host, target_nodeName, sock = self.getParams(target)
			send_all_encrypted(sock, self.message_delete_data)
			send_all_encrypted(sock, data)

		logging.debug(stampedMsg('broadCast file data deletion: {}'.format(data)))

	# target should be in membershiplist key format (groupID)
	def broadCastFile_delete(self, targets, filename):
		for target in targets:
			target_host, target_nodeName, sock = self.getParams(target)
			send_all_encrypted(sock, self.message_delete_file)
			send_all_encrypted(sock, filename)
			logging.debug(stampedMsg('{} asking for deletion of file {} to node {}'.format(self.nodeName, filename, target_nodeName)))

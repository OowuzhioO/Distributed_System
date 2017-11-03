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

		monitor = threading.Thread(target=self.server_task)
		monitor.daemon=True
		monitor.start()

	def server_task(self):
		#first, start local timer, the rest of the process follows this timer
		self.timer.tic()


		# a monitor receive message, check and response, also multicase failure message
		self.monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.monitor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.monitor.bind((self.host, self.port))
		self.monitor.listen(5)


		# self.monitor.listen(10) # UDP doesn't support this
		logging.info(stampedMsg('Monitoring process opens.'))
		
		# keep receiving msgs from other VMs
		# receiving heartbeat and other messages 

		# pdb.set_trace()
		while True:
			try:
				conn, addr = self.monitor.accept()
				rmtHost= socket.gethostbyaddr(addr[0])[0]
				logging.debug(stampedMsg('Monitor recieve instruction from {}').format(rmtHost))
			
			except socket.error, e:
				logging.warning("Caught exception socket.error : %s" %e)
				logging.warning(stampedMsg('Fail to receive signal from clients {}'.format(rmtHost)))
				break #TODO: should we break listening if UDP reception has troubles?

			data = receive_all_decrypted(conn)
			if not data: # possibly never called in UDP					
				logging.info(stampedMsg('Receiving stop signal from clients {}'.format(rmtHost)))
				break

			# log whatever recieved
			logging.debug(stampedMsg(data))

			if data == self.message_file: # if receive leave signal
				# include filename info for debugging purposes
				filename = receive_all_to_target(conn)
				logging.info(stampedMsg('receiving file {} from {}'.format(filename, rmtHost)))
				self.local_file_info[filename] = datetime.datetime.now()

			elif data == self.message_data: # ....
				filename, file_nodes = receive_all_decrypted(conn)
				self.global_file_info[filename] = (self.timer.toc(), file_nodes) # time should sent over instead of local
				# process info next

		return None 

	def getFile(self, filename):
		# check metadata first
		pass

	def putFile(self, filename):
		if (filename in self.global_file_info):
			# broadcast to that group
			target_processes = [node for node in self.global_file_info[filename][-1] if node != self.groupID]
			self.broadCastFile(target_processes, filename)
			# simple synchronization assumption
			
		else:
			target_processes = random.sample(self.membList.keys(), min(self.w_quorum, len(self.membList)))
			if self.groupID not in target_processes:
				target_processes = target_processes[1:]+[self.groupID]	
			self.broadCastFile(target_processes, filename) # don't broadcast to itself
			self.broadCastData(self.membList.keys(), (filename, target_processes))

	def deleteFile(self, filename):
		pass


	def onProcessFail(self, left_process):
		# do re-replicate
		pass

	# target should be in membershiplist key format (groupID)
	def broadCastData(self, targets, data):
		for target in targets:
			target_hostname = target.split('_')[0]
			target_host = socket.gethostbyname(target_hostname)
			target_nodeName = self.VM_DICT[target_hostname]
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((target_host, self.port))
			
			send_all_encrypted(sock, self.message_data)
			send_all_encrypted(sock, data)

		logging.debug(stampedMsg('broadCast Data: {}'.format(data)))

	# target should be in membershiplist key format (groupID)
	def broadCastFile(self, targets, filename):
		for target in targets:
			target_hostname = target.split('_')[0]
			target_host = socket.gethostbyname(target_hostname)
			target_nodeName = self.VM_DICT[target_hostname]
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((target_host, self.port))
			
			logging.debug(stampedMsg('{} pushing file {} to node {}({})'.format(self.nodeName, filename, target_host, target_nodeName)))
			send_all_encrypted(sock, self.message_file)
			send_all_from_file(sock, filename)

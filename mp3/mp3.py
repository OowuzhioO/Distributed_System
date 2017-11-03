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
import random


VM_DICT={}
VM_DICT.update({socket.gethostname():'localhost'})



def stampedMsg(msg):
	return strftime("[%Y-%m-%d %H:%M:%S] ", localtime())+msg


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



### Heartbeat failure detector object ###
class distributed_file_system(object):
	#  added membList and changed host name, tFail
	def __init__(self, hostName, VM_DICT,tFail, tick ,introList, randomthreshold, num_N =3, membList):
		## input hostName -- this node's host name e.g 'fa17-cs425-g57-01.cs.illinois.edu'
		## VM_DICT -- mapping host name to node name
		## tFail -- FD detector protocal period time
		## introList -- pre-selected introducer list by node names

		self.hostName=hostName
		self.VM_DICT = VM_DICT
		self.VM_INV = {v:k for k,v in VM_DICT.items()} # inverse dict of VM_DICT
		self.nodeName = self.VM_DICT[self.hostName]
		self.introList = introList
		self.host = socket.gethostbyname(self.hostName)
		logging.debug("{} has port {}".format(hostName, port))
		self.meta_port = 5363
		self.file_port = 1453
		self.randomthreshold = randomthreshold
		self.bufsize = 4096
		# membership list, passed in reference so can know the current members even within the class
		# However can't change it and should not use it to check churn
		# Instead each churn should call the corresponding function of this class
		self.membList = membList

		# a list of information about file
		self.metadata = []

		# misc settings
		self.tFail = tFail
		self.tCleanUp = 2*self.tFail
		self.timer = Timer() # use custimized timer instead of time.time

		#only a introducer can add node to system
		self.isIntro = True if self.nodeName in self.introList else False

		self.num_N=num_N
		self.neighbors = []
		self.tick = tick

		# initlize group id, later to be changed by self.joinGrp()
		self.groupID = self.hostName
		# token for introduction and leave
		self.str_receive_file = 'The following is for file content'

	# strip necessary info from membership list
	def prepMsg(self):
		strippedMembList = defaultdict(dict)
		for memb in self.membList:
			strippedMembList[memb] = {'count': self.membList[memb]['count']}
		
		msg=self.encodeMsg(strippedMembList)
		return msg


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
				instruction = receive_all_decrypted(conn) # receive string json string				
				rmtHost= socket.gethostbyaddr(addr[0])[0]
				logging.debug(stampedMsg('Monitor recieve instruction from {}').format(rmtHost))
			
			except socket.error, e:
				logging.warning("Caught exception socket.error : %s" %e)
				logging.warning(stampedMsg('Fail to receive signal from clients {}'.format(rmtHost)))
				break #TODO: should we break listening if UDP reception has troubles?

			if not data: # possibly never called in UDP					
				logging.info(stampedMsg('Receiving stop signal from clients {}'.format(rmtHost)))
				break

			# log whatever recieved
			logging.debug(stampedMsg(data))

			if data == self.str_receive_file: # if receive leave signal
				# include filename info for debugging purposes
				logging.info(stampedMsg('receiving file {} from {}'.format(filename, rmtHost)))


			else: # ....
				pass

		return None 

	def getFile(self, filename):
		# check metadata first
		pass

	def putFile(self, filename):
		pass

	def deleteFile(self, filename):
		pass


	def onProcessFail(self, left_process):
		# do re-replicate
		pass


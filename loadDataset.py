#!/usr/bin/python

import os
import time
from subprocess import *
import sys
import datetime
import socket
import json
#import numpy as np
#import pylab as plt

# nodes = [ 512, 1024, 2048 , 4096 ]
nodes = [ 128, 512, 1024 ]
modes = ['cetus_newtop']
baseDir = 'projects/ExaHDF5/mlewis/hiero'
executables = ['No Leader', 'Fixed Plane', 'Smallest Plane']
prog_executables = [ 'noleader', 'fixedplane', 'smallestplane' ]

pgm = 'loadTriple'
json_dataset = [ 'dbpedia_dataset.json' ]
outputFile = '/projects/ExaHDF5/mlewis/wiki/output/output.txt'
project = 'ExaHDF5'
data = []
resourceGroupFile = 'resourceGroup.txt'
predicateGroupFile = 'predicateGroup.txt'
ntDataset = '/projects/ExaHDF5/mlewis/wiki/dbpedia_dataset.txt'



#  loadTriple < addSchemaFiles <dataset name> | IncludeSchema <datasetfile> [ removeSchemaFiles <datasetfile> ] > <output file> 

def executeScript (node,  dataset, resourceGroupFile, predicateGroupFile):
	script = './' + pgm + '.sh ' + str(node)  + ' ' + dataset + ' ' + resourceTyping + ' ' + predicateGrouping
        cmd = 'qsub -A ' + project + ' -t 00:20:00 -n '+str(node) +' --mode script ' + script
        print 'Executing ' + cmd
	jobid = Popen(cmd, shell=True, stdout=PIPE).communicate()[0]
	print 'Jobid : ' + jobid
	while True:
		cmd = 'qstat ' + jobid.strip() + ' | grep mlewis | awk \'{print $1}\''
		jobrun = Popen(cmd, shell=True, stdout=PIPE).communicate()[0]
		if jobrun == '':
			break
		time.sleep(45)
	return jobid.strip()




ts = time.time()
timestamp = datetime.datetime.fromtimestamp(ts).strftime('%m-%d-%H-%M')

for node in nodes :
   output = executeScript(node, ntDataset, resourceGroupFile, predicateGroupFile)

  

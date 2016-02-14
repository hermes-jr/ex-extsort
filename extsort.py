#!/usr/bin/python2.7
"""
The MIT License (MIT)

Copyright (c) 2016 Mikhail Antonov

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import os
import sys
import Queue

def dumptofile(dfile, dlist):
	for item in dlist:
		dfile.write("{}\n".format(item))

""" Use about 60% of total memory by default"""
memTotal = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
memAvail = int(0.6 * memTotal)

""" ...or less """
#memAvail = 1.4 * 1073741824 # ~1.4G
memAvail = 400 * 1048576 # ~400M
#memAvail = 100 * 1048576 # ~100M

sourceFile = open('./data.dat', 'r')

currentChunkData = []
chunksNum = 0
usedSize = sys.getsizeof(currentChunkData)

# phase 1: create lots of sorted chunks
while(True):
	rawLine = sourceFile.readline()
	line = rawLine.rstrip('\n').rstrip('\r')

	if(rawLine != ''):
		""" Total memory used by list is about: 36 + 16 * line.len() bytes """
		lineSize = sys.getsizeof(line) + 16
		usedSize += lineSize
		currentChunkData.append(line)
		
		""" This one should be earlier in code, but would make things a bit complicated.
			Most lines are about the same size in this example """
		if(usedSize + lineSize * 2 < memAvail):
			continue

	""" Allocated memory is full, now sort the dataset in memory with anyconventional algorithm """
	currentChunkData.sort()

	chunkFile = open('./chunk.{}'.format(chunksNum), 'w+')
	dumptofile(chunkFile, currentChunkData)
	chunkFile.close()

	""" Free memory, reset used memory counter """
	del currentChunkData[:]
	usedSize = sys.getsizeof(currentChunkData)
	chunksNum += 1

	""" EOF """
	if(rawLine == ''):
		break

# phase 2: combine chunks
q = Queue.PriorityQueue()

resultFile = open('./result.dat', 'a+')

chunkFiles = dict()

""" Priority queue holds a first line for each pre-sorted chunk (min value in each file) """
for iter in xrange(chunksNum):
	chunkFiles.update({iter: open('./chunk.{}'.format(iter), 'r')})
	q.put((chunkFiles[iter].readline().rstrip('\n').rstrip('\r'), iter))

""" Do less writes to resulting file, use batches """
currentChunkData = []
usedSize = sys.getsizeof(currentChunkData)

while not q.empty():
	first = q.get() # Returns min value in priority queue

	usedChunk = first[1]
	usedStr = first[0]

	# Write speedup
	""" Again, total memory used by list is about: 36 + 16 * line.len() bytes """
	lineSize = sys.getsizeof(usedStr) + 16
	usedSize += lineSize
	currentChunkData.append(usedStr)

	""" Dump data to disk only if all memory is used """
	if(usedSize + lineSize * 2 >= memAvail):
		dumptofile(resultFile, currentChunkData)

		""" Free memory, reset used memory counter """
		del currentChunkData[:]
		usedSize = sys.getsizeof(currentChunkData)

	newl = chunkFiles[usedChunk].readline()
	if(newl != ""):
		q.put((newl.rstrip('\n').rstrip('\r'), usedChunk))
	else:
		chunkFiles[usedChunk].close()
		os.remove('./chunk.{}'.format(usedChunk))

""" Another dirty workaround: flush remaining data """
dumptofile(resultFile, currentChunkData)

resultFile.close()

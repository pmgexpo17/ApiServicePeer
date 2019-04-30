# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
from aiohttp.web import json_response, StreamResponse
import simplejson as json
import logging
import os
import pickle

logger = logging.getLogger('apipeer.smart')

# -------------------------------------------------------------- #
# PurgeDataSet
# ---------------------------------------------------------------#
class PurgeDataSet:

  def __init__(self, leveldbHash, *argv):
    self.db = leveldbHash

  def __call__(self, startKey, endKey):
    logger.info('!! Delete All Keys : %s, %s !!' % (startKey, endKey))
    #batch = leveldb.WriteBatch()
    batch = self.db.getBatch()
    keyIter = self.db.select(startKey, endKey, incValue=False)
    try:
      while True:
        key = keyIter.next()
        batch.Delete(key)
    except StopIteration:
      #self._leveldb.Write(batch, sync=True)
      self.db.putBatch(batch)
    
# -------------------------------------------------------------- #
# DlmrStreamPrvdr
# ---------------------------------------------------------------#
class DlmrStreamPrvdr:

  def __init__(self, leveldbHash):
    self.db = leveldbHash

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  def evalStream(self, dlm, itemIter):
    hasNext = True
    try:
      key = None
      while hasNext:
        key, item = itemIter.next()
        row = dlm.join(pickle.loads(item))
        yield row + '\n'
    except StopIteration:
      if key is None:
        raise Exception('range query returned an empty result set')

	# -------------------------------------------------------------- #
	# render a stream generator
	# ---------------------------------------------------------------#
  def __call__(self, dlm, startKey, endKey):
    logger.info('!! Render Stream : %s, %s !!' % (startKey, endKey))
    itemIter = self.db.select(startKey, endKey)

    try:
      response = Response(content_type='application/octet-stream', status=201)
      response.enable_chunked_encoding()
      response.body = self.evalStream(itemIter)
    except Exception as ex:
      return json_response({'error': str(ex),'status': 500}, status=500)
    else:
      return response

# -------------------------------------------------------------- #
# TxtStreamPrvdr
# ---------------------------------------------------------------#
class TxtStreamPrvdr:

  def __init__(self, leveldbHash):
    self.db = leveldbHash

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  def evalStream(self, itemIter):
    hasNext = True
    try:
      key = None
      while hasNext:
        key, item = itemIter.next()
        yield pickle.loads(item) + '\n'
    except StopIteration:
      if key is None:
        raise Exception('range query returned an empty result set')

	# -------------------------------------------------------------- #
	# render a stream generator
	# ---------------------------------------------------------------#
  def __call__(self, startKey, endKey):
    logger.info('!! Render Stream : %s, %s !!' % (startKey, endKey))
    itemIter = self.db.select(startKey, endKey)

    result = {'contentType' : 'bytes','chunked' : True,'status': 201}

    try:
      response = Response(content_type='application/octet-stream', status=201)
      response.enable_chunked_encoding()
      response.body = self.evalStream(itemIter)
    except Exception as ex:
      return {'error': str(ex),'status': 500}, 500
    return result, 201

# -------------------------------------------------------------- #
# TxtFileStreamPrvdr
# ---------------------------------------------------------------#
class TxtFileStreamPrvdr(object):

  def __init__(self, leveldbHash):
    self.db = leveldbHash

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  def evalStream(self, txtFilePath):

    with open(txtFilePath, 'rb') as fhr:
      while True:        
        chunk = fhrb.read(1024)
        if not chunk:
          break
        yield chunk

	# -------------------------------------------------------------- #
	# render a text file stream generator
	# ---------------------------------------------------------------#
  def __call__(self, jobId, txtFileName):
    logger.info('!! Render Text File Stream, jobId : %s !!' % jobId)
    try:
      dbKey = '%s|TASK|workspace' % jobId
      self.workSpace = self.db[dbKey]
    except KeyError:
      errMsg = '%s workspace not found' % jobId
      return json_response({'error': errMsg,'status': 400}, status=400)

    txtFilePath = '%s/%s' % (self.workSpace, txtFileName)
    if not os.path.exists(txtFilePath):
      errMsg = 'file does not exist : %s' % txtFilePath
      return json_response({'error': errMsg,'status': 400}, status=400)

    try:
      response = Response(content_type='application/octet-stream', status=201)
      response.enable_chunked_encoding()
      response.body = self.evalStream(txtFilePath)
    except Exception as ex:
      return json_response({'error': str(ex),'status': 500}, status=500)
    else:
      return response

# -------------------------------------------------------------- #
# BinryFileStreamPrvdr
# ---------------------------------------------------------------#
class BinryFileStreamPrvdr(object):

  def __init__(self, leveldbHash):
    self.db = leveldbHash

	# -------------------------------------------------------------- #
	# evalStream
	# ---------------------------------------------------------------#
  async def evalStream(self, binryFilePath):

    with open(binryFilePath, 'rb') as fhrb:
      # for better memory management
      self.response.enable_chunked_encoding()
      while True:        
        chunk = fhrb.read(1024)
        if not chunk:
          break
        await self.response.write(chunk)
      await self.response.write_eof()
      return self.response

	# -------------------------------------------------------------- #
	# render a binary file stream generator
	# ---------------------------------------------------------------#
  async def __call__(self, jobId, binryFileName):
    logger.info('!! Render Binary File Stream, jobId : %s !!' % jobId)
    try:
      dbKey = '%s|workspace' % jobId
      self.workSpace = self.db[dbKey]
    except KeyError:
      errMsg = '%s workspace not found' % jobId
      return json_response({'error': errMsg,'status': 400}, status=400)

    binryFilePath = '%s/%s' % (self.workSpace, binryFileName)

    if not os.path.exists(binryFilePath):
      errMsg = 'file does not exist : %s' % binryFilePath
      return json_response({'error': errMsg,'status': 400}, status=400)

    try:
      return await self.evalStream(binryFilePath)
    except Exception as ex:
      return json_response({'error': str(ex),'status': 500}, status=500)

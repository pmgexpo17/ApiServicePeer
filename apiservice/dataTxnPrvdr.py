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
from flask import Response
import json
import logging
import leveldb
import os

logger = logging.getLogger('apscheduler')

# -------------------------------------------------------------- #
# DelDataUnit
# ---------------------------------------------------------------#
class DelDataUnit(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

  def __call__(self, startKey, endKey):
    keyIter = self._leveldb.RangeIter(startKey, endKey, include_value=False)
    while True:
      try:
        key = keyIter.next()
        self._leveldb.Delete(key)
      except StopIteration:
        break

# -------------------------------------------------------------- #
# DataStreamPrvdr
# ---------------------------------------------------------------#
class DlmrStreamPrvdr(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb
    
	# -------------------------------------------------------------- #
	# render a stream generator
	# ---------------------------------------------------------------#
  def __call__(self, dlm, startKey, endKey):
    logger.info('RENDER_STREAM : %s, %s' % (startKey, endKey))
    itemIter = self._leveldb.RangeIter(startKey, endKey)
    def generate():
      while True:
        try:
          key, item = itemIter.next()
          try:
            row = dlm.join(json.loads(item))
          except Exception as ex:
            logger.error('json loads failed : ' + str(ex))
            raise
          yield row + '\n'
        except StopIteration:
          break
    return Response(generate(), status=201, mimetype='text/html')


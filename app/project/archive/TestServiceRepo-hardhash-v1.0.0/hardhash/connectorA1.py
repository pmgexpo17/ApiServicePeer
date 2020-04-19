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
from threading import Lock, RLock
import leveldb
import pickle
import os, sys
import subprocess

try:
  DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL
except AttributeError:
  DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL

# ---------------------------------------------------------------------------#
# HardhashA
# ---------------------------------------------------------------------------#    
class HardhashA():
  _instance = None

  def __init__(self, dbPath, pickleProtocol=DEFAULT_PROTOCOL):
    if not os.path.exists(dbPath):
      raise Exception('leveldb directory does not exist : ' + dbPath)
    self._instance = leveldb.LevelDB(dbPath)
    self._protocol = pickleProtocol
    self._lock = RLock()

	#------------------------------------------------------------------#
	# get
	#------------------------------------------------------------------#
  @classmethod
  def get(cls):
    return cls._instance

  #----------------------------------------------------------------#
  # __getitem__
  # for non-bytes type values
  #----------------------------------------------------------------#		
  def __getitem__(self, key):
    return pickle.loads(self._leveldb.Get(key.encode()))

  #----------------------------------------------------------------#
  # __setitem__
  #----------------------------------------------------------------#		
  def __setitem__(self, key, value):
    self.put(key,value)
    
  #----------------------------------------------------------------#
  # __delitem__
  #----------------------------------------------------------------#		
  def __delitem__(self, key):
    self._leveldb.Delete(key.encode())

  #----------------------------------------------------------------#
  # bytes
  # restores a bytes or bytearray value
  #----------------------------------------------------------------#		
  def bytes(self, key):
    return self._leveldb.Get(key.encode())

  #----------------------------------------------------------------#
  # put
  #----------------------------------------------------------------#		
  def put(self, key, value, sync=False):
    with self._lock:
      if not isinstance(value, (bytes, bytearray)):
        # use pickle to handle any kind of object value
        value = pickle.dumps(value, self._protocol)
      self._leveldb.Put(key.encode(), value, sync=sync)

  #----------------------------------------------------------------#
  # select
  #----------------------------------------------------------------#		
  def select(self, startKey, endKey, incValue=True):
    dbIter = self._leveldb.RangeIter(startKey.encode(), endKey.encode(), include_value=incValue)
    return ResultSet(dbIter, incValue)

#----------------------------------------------------------------#
# ResultSet
#----------------------------------------------------------------#		
class ResultSet():
  def __init__(self, dbIter, incValue):
    self.dbIter = dbIter
    self.__next__ = self.__nextVal if incValue else self.__nextKey

  def __iter__(self):
    return self

  def __nextVal(self):
    key, value = self.dbIter.__next__()
    return (key.decode(), pickle.loads(value)) 

  def __nextKey(self):
    key = self.dbIter.__next__()
    return key.decode()

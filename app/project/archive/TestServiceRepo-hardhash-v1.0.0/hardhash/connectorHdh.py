__all__ = [
  'DatastoreError',
  'EmptyViewResult',
  'HHRequest',
  'HHResponse']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, AbstractConnector, Packet
from collections import deque, OrderedDict
import asyncio
import logging
import time
import zmq
import pickle

try:
  DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL
except AttributeError:
  DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL

logger = logging.getLogger('asyncio.hardhash')

# -------------------------------------------------------------- #
# DatastoreError
# ---------------------------------------------------------------#
class DatastoreError(Exception):
  def __init__(self, errobj):
    if isinstance(errobj, dict):
      message = errobj.pop('message')
      self.__dict__.update(errobj)
      super().__init__(message)
    else:
      super().__init__(errobj)

#----------------------------------------------------------------#
# EmptyViewResult
#----------------------------------------------------------------#		
class EmptyViewResult(Exception):
  pass

#----------------------------------------------------------------#
#----------------- Hardhash Request Connectors ------------------#
#----------------------------------------------------------------#

#----------------------------------------------------------------#
# HHReqConnector - partially fills MutableMapping interface
#----------------------------------------------------------------#		
class HHReqConnector(AbstractConnector):
  def __init__(self, sockware, pickleMode=DEFAULT_PROTOCOL):
    super().__init__(sockware.socket)
    self.sockAddr = sockware.address
    self.pickleMode = pickleMode

  #----------------------------------------------------------------#
  # recv - for asyncio integration, recv returns an awaitable future
  #----------------------------------------------------------------#		
  async def recv(self, flags=0):
    try:
      return pickle.loads(await self.sock.recv(flags=flags))
    except EOFError:
      return ''

  #----------------------------------------------------------------#
  # send - for asyncio integration, send_multipart returns an 
  # awaitable future. Payload must not be in (bytes, bytearray)
  #----------------------------------------------------------------#		
  async def send(self, packet, payload=None):    
    packet = [item.encode() for item in packet]      
    if payload:
      # use pickle to handle any kind of object value
      try:
        payload = pickle.dumps(payload, self.pickleMode)
      except pickle.PickleError as ex:
        errmsg = f'{self.name}, pickle error, packet : {packet}'
        raise DatastoreError(f'{errmsg}, unserializable payload : {payload}')
      packet.append(payload)
    await self.sock.send_multipart(packet)

  #----------------------------------------------------------------#
  # MutableMapping methods for emulating a dict
  #----------------------------------------------------------------#		

  #----------------------------------------------------------------#
  # __getitem__
  # this connector is potentially a shared resource
  # use lock to ensure the send and recv combination is atomic
  #----------------------------------------------------------------#		
  async def __getitem__(self, key):
    try:
      await self.send(['GET',key])
      return await self.recv()
    except Exception as ex:
      logger.error(f'GET failed, dbkey : {key}', exc_info=True)
      raise DatastoreError(ex)

  #----------------------------------------------------------------#
  # __setitem__
  #----------------------------------------------------------------#		
  def __setitem__(self, key, value):
    try:
      logger.info(f'{self.name}, PUT key : {key}')
      coro = self.send(['PUT',key],payload=value)
      asyncio.ensure_future(coro)
    except Exception as ex:
      logger.error(f'PUT failed, dbkey : {key}', exc_info=True)
      raise DatastoreError(ex)

  #----------------------------------------------------------------#
  # __delitem__
  #----------------------------------------------------------------#		
  def __delitem__(self, key):
    try:
      coro = self.send(['DELETE',key])
      asyncio.ensure_future(coro)
    except Exception as ex:
      logger.error(f'DELETE failed, dbkey : {key}', exc_info=True)
      raise DatastoreError(ex)

  #----------------------------------------------------------------#
  # getGenerator
  #----------------------------------------------------------------#		
  async def getGenerator(self, keyLow, keyHigh, keysOnly):
    await self.send(['SELECT',keyLow,keyHigh,str(keysOnly)])
    value = await self.recv()
    yield value
    while self.sock.getsockopt(zmq.RCVMORE):
      value = await self.recv(zmq.RCVMORE)
      yield value

  #----------------------------------------------------------------#
  # add
  #----------------------------------------------------------------#		
  async def add(self, key, value):
    try:
      await self.send(['ADD',key],payload=value)
    except Exception as ex:
      logger.error(f'ADD failed, dbkey : {key}', exc_info=True)
      raise DatastoreError(ex)

  #----------------------------------------------------------------#
  # append
  #----------------------------------------------------------------#		
  async def append(self, key, value):
    try:
      await self.send(['APPEND',key],payload=value)
    except Exception as ex:
      logger.error(f'APPEND failed, dbkey : {key}', exc_info=True)
      raise DatastoreError(ex)

  #----------------------------------------------------------------#
  # combine
  #----------------------------------------------------------------#		
  async def combine(self, nodeName, payload):
    try:
      if not nodeName:
        nodeName = ''
      await self.send(['COMBINE',nodeName],payload=payload)
    except Exception as ex:
      logger.error(f'COMBINE failed, dbkeys : {nodeName}', exc_info=True)
      raise DatastoreError(ex)

  #----------------------------------------------------------------#
  # put
  #----------------------------------------------------------------#		
  async def put(self, key, value):
    try:
      await self.send(['PUT',key],payload=value)
    except Exception as ex:
      logger.error(f'PUT failed, dbkey : {key}', exc_info=True)
      raise DatastoreError(ex)

  #----------------------------------------------------------------#
  # select
  # this connector is potentially a shared resource
  # use lock to ensure the generator coroutine happens atomically
  #----------------------------------------------------------------#		
  def select(self, keyLow, keyHigh, keysOnly=False):
    try:
      logger.info(f'SELECT keyLow, keyHigh : {keyLow},{keyHigh}')
      return self.getGenerator(keyLow, keyHigh, keysOnly)
    except Exception as ex:
      logger.error(f'SELECT failed, keyLow, keyHigh : {keyLow}, {keyHigh}', exc_info=True)
      raise DatastoreError(ex)

#----------------------------------------------------------------#
# HHRequest
#----------------------------------------------------------------#		
class HHRequest(HHReqConnector):
  def __init__(self, sockware, **kwargs):
    super().__init__(sockware, **kwargs)
    self.serialize = 'PYOBJ'

  #----------------------------------------------------------------#
  # recv
  #----------------------------------------------------------------#		
  async def recv(self, flags=0):
    value = await super().recv(flags)      
    if self.serialize == 'JSON':
      return json.dumps(value)
    return value

  #----------------------------------------------------------------#
  # serializeBy
  #----------------------------------------------------------------#		
  def serializeBy(self, mode):
    self.serialize = mode

  #----------------------------------------------------------------#
  # notify
  #----------------------------------------------------------------#		
  async def notify(self, taskId, owner):
    logger.info(f'{self.cid}.{taskId}, task completion notified by owner {owner}')
    await self.send(['NOTIFY', taskId])

#----------------------------------------------------------------#
#----------------- Hardhash Response Connectors -----------------#
#----------------------------------------------------------------#

#----------------------------------------------------------------#
# asyncify
#----------------------------------------------------------------#		
def asyncify(func):
  def wrapper(*args, **kwargs):
    f = asyncio.Future()
    try:
      result = func(*args, **kwargs)
    except Exception as ex:
      f.set_exception(ex)
    else:
      f.set_result(result)
    finally:
      return f
  return wrapper

#----------------------------------------------------------------#
# HHRespConnector
#----------------------------------------------------------------#		
class HHRespConnector(AbstractConnector):
  def __init__(self, sockware, pickleMode=DEFAULT_PROTOCOL):
    super().__init__(sockware.socket)
    self.sockAddr = sockware.address
    self.pickleMode = pickleMode

  #----------------------------------------------------------------#
  # recv
  #----------------------------------------------------------------#		
  def recv(self):
    return self.sock.recv_multipart()

  #----------------------------------------------------------------#
  # send - client expects to receive a pickled array
  #----------------------------------------------------------------#		
  async def send(self, payload, flags=0):
    await self.sock.send(payload,flags=flags)

#----------------------------------------------------------------#
# HHResponse
#----------------------------------------------------------------#		
class HHResponse(HHRespConnector):
  def __init__(self, sockware, leveldb, **kwargs):
    super().__init__(sockware, **kwargs)
    self._leveldb = leveldb
    self.__dict__['APPEND'] = self._APPEND
    self.__dict__['COMBINE'] = self._COMBINE
    self.__dict__['DELETE'] = self._DELETE
    self.__dict__['GET'] = self._GET
    self.__dict__['NULL'] = self._NULL
    self.__dict__['PUT'] = self._PUT
    self.__dict__['SELECT'] = self._SELECT
    self._combiner = {}
    self.runMode = logging.INFO

  def __getitem__(self, request):
    return self.__dict__[request]

  #----------------------------------------------------------------#
  # _NULL
  #----------------------------------------------------------------#		
  def _NULL(self):
    pass

  #----------------------------------------------------------------#
  # debugOption
  #----------------------------------------------------------------#		
  def debugOption(self, request, key):
    logger.info(f'{self.name}, {request} : {key.decode()}')

  #----------------------------------------------------------------#
  # query
  #----------------------------------------------------------------#		
  def query(self, key):
    try:
      return self._leveldb.Get(key)
    except KeyError:
      return None

  #----------------------------------------------------------------#
  # _ADD
  #----------------------------------------------------------------#		
  @asyncify
  def _ADD(self, key, value):
    logger.info(f'{self.name}, got key : {key.decode()}')
    self._addHandler.add(key, value)

  #----------------------------------------------------------------#
  # _GET
  #----------------------------------------------------------------#		
  async def _GET(self, key):
    try:
      payload = self._leveldb.Get(key)
      await self.send(payload)
    except KeyError:
      await self.send(pickle.dumps(None,self.pickleMode))
      ### TO DO : raise Exception for handling by a control channel

  #----------------------------------------------------------------#
  # _PUT
  #----------------------------------------------------------------#
  @asyncify
  def _PUT(self, key, value):    
    self._leveldb.Put(key, value)

  #----------------------------------------------------------------#
  # _SPUT - serialize first
  #----------------------------------------------------------------#
  def _SPUT(self, key, value):
    self._leveldb.Put(key.encode(), pickle.dumps(value,self.pickleMode))

  #----------------------------------------------------------------#
  # _APPEND
  # TODO : For handling large datasets, array bytesize must be checked
  # and if sizeLimit is exceeded, create a linked list array. That
  # is, the last array element is a next pointer
  #----------------------------------------------------------------#		
  @asyncify
  def _APPEND(self, key, value):
    try:
      valueA = self._leveldb.Get(key)
    except KeyError:
      valueA = []
    else:
      valueA = pickle.loads(valueA)
    valueA.append(value)
    self._leveldb.Put(key, pickle.dumps(valueA, self.pickleMode))

  #----------------------------------------------------------------#
  # _COMBINE_
  #----------------------------------------------------------------#		
  @asyncify
  def _COMBINE(self, bnodeName, bpayload):
    payload = pickle.loads(bpayload)
    if bnodeName == b'':
      nodeName, combiner = Combiner.make(self._SPUT, payload)
      self._combiner[nodeName] = combiner
      return
    nodeName = bnodeName.decode()
    combiner = self._combiner[nodeName]
    modeKey = payload.pop(0)
    combiner[modeKey](*payload)
    if combiner.done:
      logger.debug(f'{self.name}, combine request {combiner.dbkey[1]} is now done')
      self._combiner.pop(combiner.nodeName)

  #----------------------------------------------------------------#
  # _DELETE
  #----------------------------------------------------------------#		
  @asyncify
  def _DELETE(self, key):
    self._leveldb.Delete(key)

  #----------------------------------------------------------------#
  # _SELECT
  #----------------------------------------------------------------#		
  async def _SELECT(self, keyLow, keyHigh, keysOnly):
    logger.info('keys only : ' + str(keysOnly))
    incValues = keysOnly.decode() != 'True'    
    wrangler = self.getWrangler(keyLow, keyHigh, incValues)
    await wrangler.run()

  #----------------------------------------------------------------#
  # getWrangler
  #----------------------------------------------------------------#		
  def getWrangler(self, keyLow, keyHigh, incValues):
    modeMsg = 'rangeVals' if incValues else 'rangeKeys'
    logger.info(f'### {modeMsg}, keyLow, keyHigh : {keyLow}, {keyHigh}')
    rangeIter = self._leveldb.RangeIter(key_from=keyLow, key_to=keyHigh,include_value=incValues)    
    if incValues:
      return RangeValueWrangler(rangeIter, self.send)
    return RangeKeyWrangler(rangeIter, self.send)

#----------------------------------------------------------------#
# RangeWrangler
#----------------------------------------------------------------#		
class RangeWrangler:
  def __init__(self, rangeIter, sendFunc):
    self.rangeIter = rangeIter
    self._send = sendFunc
    self._next = None

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  #----------------------------------------------------------------#
  # run
  # manages the pyzmq SNDMORE protocol, by using a buffer, size = 2
  # which eventually holds the 2nd last and last part of the sequence
  # SNDMORE goes with 2nd last, then finally send the last item
  #----------------------------------------------------------------#		
  async def run(self):
    self.first = True    
    self.buffer = deque(maxlen=2)    
    if not self.hasFirst:
      await self.send()
      return
    while self.hasMore:
      await self.send(flags=zmq.SNDMORE)
    await self.send() # send last one

  #----------------------------------------------------------------#
  # nextValue
  #----------------------------------------------------------------#		
  def nextValue(self):
    key, value = self.rangeIter.__next__()
    return value

  #----------------------------------------------------------------#
  # nextKey
  #----------------------------------------------------------------#		
  def nextKey(self):
    return self.rangeIter.__next__()

  #----------------------------------------------------------------#
  # hasFirst
  #----------------------------------------------------------------#		
  @property
  def hasFirst(self):
    try:
      value = self._next()
    except StopIteration:
      self.buffer.appendleft(b'')
      return False
    else:
      self.buffer.append(value)
      return True
    finally:
      self.first = False

  #----------------------------------------------------------------#
  # hasMore
  #----------------------------------------------------------------#
  @property
  def hasMore(self):
    try:
      value = self._next()
    except StopIteration:
      # special case : one item
      if len(self.buffer) == 1:
        self.buffer.rotate()
      return False
    else:
      self.buffer.append(value)
      return True

  #----------------------------------------------------------------#
  # send
  #----------------------------------------------------------------#		
  async def send(self, flags=0):
    value = self.buffer.popleft()
    await self._send(value, flags=flags)

#----------------------------------------------------------------#
# RangeValueWrangler
#----------------------------------------------------------------#		
class RangeValueWrangler(RangeWrangler):
  def __init__(self, *args):
    super().__init__(*args)
    self._next = self.nextValue

#----------------------------------------------------------------#
# RangeKeyWrangler
#----------------------------------------------------------------#		
class RangeKeyWrangler(RangeWrangler):
  def __init__(self, *args):
    super().__init__(*args)
    self._next = self.nextKey

#----------------------------------------------------------------#
# Combiner
#----------------------------------------------------------------#		
class Combiner:
  def __init__(self, putFunc, payload):
    self._put = putFunc
    self._cache = {}
    self.done = False
    self.apply(payload)

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  @classmethod
  def make(cls, putFunc, payload):
    combiner = cls(putFunc, payload)
    return (payload['nodeName'], combiner)

  def __getitem__(self, modeKey):
    try:
      func = f'add_{modeKey}'
      return getattr(self, func)
    except AttributeError:
      raise DatastoreError(f'{self.name}, combiner mode key {modeKey} is invalid')

  #----------------------------------------------------------------#
  # add_columns
  #----------------------------------------------------------------#
  def add_columns(self, nodeName, columns):
    if not nodeName:
      nodeName = self.nodeName    
    logger.debug(f'{self.name}, adding {nodeName} columns ...')    
    self._cache[nodeName] = [columns]

  #----------------------------------------------------------------#
  # add_record
  #----------------------------------------------------------------#
  def add_record(self, nodeName, record):
    if not nodeName:
      nodeName = self.nodeName
      logger.info(f'{self.name}, loading {self.dbkey[1]} combination ...')
      self._cache[nodeName].append(record)
      # write the header and data records
      self._write(0)
      self._write(1)
      self.done = True
    else:
      self._cache[nodeName].append(record)

  #----------------------------------------------------------------#
  # _write
  #----------------------------------------------------------------#
  def _write(self, index):
    record = []
    for key in self.order:
      record.extend(self._cache[key][index])
    self._put(self.dbkey[index], record)

  #----------------------------------------------------------------#
  # apply
  #----------------------------------------------------------------#
  def apply(self, payload):
    # __dict__.update installs :
    # nodeName : the dbkey owner
    # dbkey : dbkey value for later combination and db write
    # order : the required segment order for later combination and db write 
    logger.debug(f'{self.name}, apply combiner payload : {payload}')
    self.__dict__.update(payload)

__all__ = (
  'ApiPacket',
  'Article',
  'JobPacket', 
  'Note',
  'Packet', 
  'DEFAULT_PROTOCOL')

import copy
import pickle
import logging

logger = logging.getLogger('asyncio.broker')

try:
  DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL
except AttributeError:
  DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL

# -------------------------------------------------------------- #
# Note
# ---------------------------------------------------------------#
class Note:
  def __init__(self, packet={}):
    self.__dict__.update(packet)

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    return getattr(self, str(key))

  @property
  def body(self):
    return self.__dict__

# -------------------------------------------------------------- #
# Article
# ---------------------------------------------------------------#
class Article(Note):

  def copy(self, pop=[], merge={}):
    clone = copy.deepcopy(self.__dict__)
    [clone.pop(popKey) for popKey in pop]
    clone.update(merge)
    return clone

  def merge(self, packet, pop=[]):
    if not isinstance(packet, dict):
      raise TypeError('dict.update requires a dict argument')
    self.__dict__.update(packet)

  def export(self, *keys):
    if not keys:
      return self.copy()
    return {key:self.__dict__[key] for key in keys if key in self.__dict__}

  def select(self, *keys):
    if not keys:
      keys = self.__dict__.keys()
    return [self.__dict__[key] for key in keys if key in self.__dict__]

# -------------------------------------------------------------- #
# Packet
# ---------------------------------------------------------------#
class Packet(Article):
  def __init__(self, packet={}):
    if 'args' not in packet:
      self.args = []
    if 'kwargs' not in packet:
      self.kwargs = {}
    super().__init__(packet)

# -------------------------------------------------------------- #
# JobPacket
# ---------------------------------------------------------------#
class JobPacket(Packet):
  def __init__(self, packet):
    if 'taskRange' in packet:
      self.evalRange(packet['taskRange'])
      del packet['taskRange']
    if 'reload' not in packet:
      self.reload = []
    super().__init__(packet)
    
  @property
  def taskset(self):
    return [f'task{taskNum:02}' for taskNum in self.taskRange]

  def evalRange(self, taskRange):
    try:
      b = int(taskRange) + 1
      self.taskRange = range(1,b)
    except ValueError:
      if '-' in taskRange:
        a,b = list(map(int,taskRange.split('-')))
        self.taskRange = range(a,b)

# -------------------------------------------------------------- #
# ApiPacket
# ---------------------------------------------------------------#
class ApiPacket(JobPacket):
  def __init__(self, packet):
    reqList = ['actor','caller','jobId','synchronous','typeKey']
    required = set(reqList)
    provided = set(packet.keys())
    if not required.issubset(provided):
      errmsg = 'one or more required job params are not included'
      raise Exception(f'{errmsg}\nrequired params: {reqList}')
    super().__init__(packet)
    self.taskKey = f'{self.jobId}:{self.typeKey}:{self.actor}'

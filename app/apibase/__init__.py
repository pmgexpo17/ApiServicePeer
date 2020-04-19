# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from logging.handlers import RotatingFileHandler
import asyncio
import importlib
import logging
import threading

logger = logging.getLogger('asyncio.server')

# -------------------------------------------------------------- #
# addHandler
# ---------------------------------------------------------------#
def addHandler(logger, logfile=None):
  if logfile:
    handler = RotatingFileHandler(logfile, maxBytes=5000000, backupCount=10)
  else:
    handler = logging.StreamHandler(sys.stdout)
  logFormat = '%(levelname)s:%(asctime)s,%(filename)s:%(lineno)d %(message)s'
  logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
  handler.setFormatter(logFormatter)
  logger.addHandler(handler)

class TaskError(Exception):
  pass

class TaskComplete(Exception):
  pass

# -------------------------------------------------------------- #
# FilePacket
# ---------------------------------------------------------------#
class FilePacket:

  @classmethod
  def open(cls, jsonFile):
    try:
      with open(jsonFile,'r') as fhr:
        packet = json.load(fhr)
      return Packet(packet)
    except (IOError, ValueError) as ex:
      logger.error(f'failed loading {jsonFile} : ' + str(ex))
      raise

import pickle
import sys
import simplejson as json
from .terminal import *
from .apiPacket import *
from .leveldbHash import LeveldbHash
from .datastore import *
from .zmqbase import *
from .jobExecutor import *
from .handler import *
from .jobControler import JobControler
from .jobProvider import JobProvider
from .appDirectorFw import *
from .jobTrader import JobTrader
from .apiServer import ApiServer

# ------------------------------------------------------------ #
# ApiRunner
# -------------------------------------------------------------#
class ApiRunner(Hashable):
  apiBase = None
  trader = None

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  # ------------------------------------------------------------ #
  # start
  # -------------------------------------------------------------#
  @classmethod
  def __start__(cls, apiBase):
    cls.apiBase = apiBase

  # ------------------------------------------------------------ #
  # run
  # -------------------------------------------------------------#
  def run(self, mode, metaDoc):
      modeKey = f'_{mode}'
      self[modeKey](metaDoc)

# ------------------------------------------------------------ #
# ResourceLoader
# -------------------------------------------------------------#
class ResourceLoader(ApiRunner):

  def __init__(self):
    self._services = self._loadModules
    self._events = self._loadEvents
    self._leveldb = LeveldbHash.get()

  def __call__(self, catalog):
    for key, item in catalog.items():
      super().run(key, item)

  # ------------------------------------------------------------ #
  # loadModules
  # -------------------------------------------------------------#
  def _loadModules(self, register):
    for category, serviceRef in register.items():
      logger.info(f'Loading {category} modules ...')  
      for module in serviceRef:
        self.loadModule(module['name'], module['fromList'])

  # ------------------------------------------------------------ #
  # loadModule
  # -------------------------------------------------------------#
  def loadModule(self, moduleName, fromList):
    try:
      sys.modules[moduleName]
    except KeyError:
      importlib.__import__(moduleName, fromlist=[fromList])

  # ------------------------------------------------------------ #
  # loadEvents
  # -------------------------------------------------------------#
  def _loadEvents(self, catalog):
    for category, fileset in catalog.items():
      logger.info(f'Loading {category} events ...')
      logger.info(f'fileset : {str(fileset)}')
      for fileName in fileset:
        self.loadEvent(category, fileName)

  # ------------------------------------------------------------ #
  # loadEvent
  # -------------------------------------------------------------#
  def loadEvent(self, category, fileName):
    eventFile = self.apiBase + f'/events/{fileName}.json'
    packet = FilePacket.open(eventFile)
    logger.info(f'loading {category} item : {packet.eventKey}')
    self._leveldb[packet.eventKey] = packet.metaDoc

# -------------------------------------------------------------- #
# ServiceRunner
# ---------------------------------------------------------------#
class ServiceRunner(ApiRunner):
  def __init__(self):
    self._services = self._join
    self._request = ApiRequest.connector('control')

  def __call__(self, catalog):
    logger.info(f'{self.name}, current thread : ' + threading.current_thread().name)
    for key, item in catalog.items():
      self.run(key, item)

  def _join(self, jobList):
    coro = self.submit(jobList)
    asyncio.ensure_future(coro)

  async def submit(self, jobList):
    try:
      for jrecord in jobList:
        jpacket = Article(jrecord)
        jobId = jpacket.jobId
        logger.info(f'{self.name}, creating job {jobId}, manifest : {jrecord}')
        response = await self.request('create', jrecord)
        logger.info(f'{self.name}, {jobId} creation result : {response}')
    except asyncio.CancelledError:
      logger.info(f'ATTN. {jobId}, creation task is canceled')
    except Exception as ex:
      logger.error(f'{jobId}, creation task task errored', exc_info=True)

  # -------------------------------------------------------------- #
  # request
  # ---------------------------------------------------------------#
  async def request(self, method, jpacket):
    await self._request.send([method, jpacket],self.name)
    if jpacket['synchronous']:
      status, response = await self._request.recv()
      if status not in (200,201):
        raise TaskError(f'{self.name}, {method} failed : {response}')
      return response

# -------------------------------------------------------------- #
# ApiLoader
# ---------------------------------------------------------------#
class ApiLoader(ApiRunner):
  def __init__(self):
    self._load = ResourceLoader()
    self._join = ServiceRunner()

	# -------------------------------------------------------------- #
	# make
	# ---------------------------------------------------------------#
  @classmethod
  def make(cls, apiBase):
    logger.info('ApiServicePeer is starting ...')
    AbstractSystemUnit.__start__(apiBase)
    JobControler.__start__(apiBase)
    LeveldbHash.__start__(apiBase)
    ApiRunner.__start__(apiBase)
    BaseExecutor.__start__()    
    return cls()
    
  # ------------------------------------------------------------ #
  # run
  # -------------------------------------------------------------#
  def run(self, resourceList):
    try:
      for metaItem in resourceList:
        catalog = FilePacket.open(f'{self.apiBase}/{metaItem}')
        self.handle(catalog)
    except Exception as ex:
      logger.error('ApiLoader errored', exc_info=True)
      
  # ------------------------------------------------------------ #
  # handle
  # -------------------------------------------------------------#
  def handle(self, catalog):
    for mode, modeList in catalog.mode.items():
      modecat = catalog.export(*modeList)
      logger.info(f'calling worker to handle {mode} resources ...')
      logger.info(f'Resources : {str(modecat)}')
      super().run(mode, modecat)

from .apiPeer import ApiPeer
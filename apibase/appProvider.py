from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from jobstore import LeveldbJobStore
from threading import RLock
import json
import logging
import leveldb
import os
import uuid

logger = logging.getLogger('apscheduler')
# -------------------------------------------------------------- #
# AppProvider
# ---------------------------------------------------------------#
class AppProvider(object):

  def __init__(self):
    self.db = None
    self.scheduler = None
    self.service = None
    self._job = None
    self.lock = RLock()

  # -------------------------------------------------------------- #
  # init
  # ---------------------------------------------------------------#
  def init(self, dbPath):

    if self.db:
      logger.info('XXXXXX init: aready done XXXXXXXXXX')
      return
    logger.info('XXXXXX init: not done XXXXXXXXXX')
    if not os.path.isdir(dbPath):
      raise BaseException("app.config['DB_PATH'] is not a directory : " + dbPath)
    _leveldb = leveldb.LevelDB(dbPath)
    self.db = _leveldb
    jobstore = LeveldbJobStore(_leveldb)
    jobstores = { 'default': jobstore } 
    scheduler = BackgroundScheduler(jobstores=jobstores)
    scheduler.start()
    self.scheduler = scheduler
    self.registry = ServiceRegister()
    self.registry.load('/home/devapps/python/ApiServicePeer/flaskApi/temp/apiservices.json')
    self._job = {}

  # -------------------------------------------------------------- #
  # addJobGroup
  # ---------------------------------------------------------------#
  def addJobGroup(self, params, jobCount):

    if not hasattr(params, 'jobId'):
      raise BaseException("required parameter 'jobId' not provided")
    self.parentJob = self._job[params.jobId]
    module, className = self.registry.getClassName(params.service)
    jobRange = range(0, jobCount)

    jobs = self.parentJob.listener.addJobs(jobRange)
    for jobNum in jobRange:
      jobId = jobs[jobNum]
      self._job[jobId] = getattr(module, className)(jobId, self.db)
      self.addJob(jobId, params, jobNum=jobNum+1)

  # -------------------------------------------------------------- #
  # addJob
  # ---------------------------------------------------------------#
  def addJob(self, jobId, params, jobNum=None):

      args = [jobId] + params.args
      kwargs = None
      if hasattr(params, 'kwargs'):
        kwargs = params.kwargs
      if jobNum:
        args += [jobNum]
      self.scheduler.add_job('apibase:runJob',id=jobId,args=args,kwargs=kwargs)

  # -------------------------------------------------------------- #
  # addNewJob
  # ---------------------------------------------------------------#
  def addNewJob(self, params):

    module, className = self.registry.getClassName(params.service)
    jobId = uuid.uuid4()
    delegate = getattr(module, className)(jobId, self.db)
    module, className = self.registry.getClassName(params.listener)    
    listener = getattr(module, className)(jobId, self.db)
    listener.state = delegate.state
    listener.addJob(jobId)
    delegate.listener = listener
    self._job[jobId] = delegate
    self.addJob(jobId, params)
    self.scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, params, jobCount=None):

    _params = Params(params)
    with self.lock:
      try:
        _params.jobId
      except AttributeError:
        self.addNewJob(_params)    
      else:
        self.addJobGroup(_params, jobCount)
  
# -------------------------------------------------------------- #
# ServiceRegister
# ---------------------------------------------------------------#
class ServiceRegister(object):

  def __init__(self):
    self._modules = None

  def load(self, apiServicesMeta):
    with open(apiServicesMeta,'r') as metaFile:
      serviceMeta = json.load(metaFile)
      self._modules = {}
      for module in serviceMeta['services']:
        self.loadModule(module['name'], module['fromList'])

  def loadModule(self, moduleName, fromList):
    try:
      self._modules[moduleName]
    except KeyError:
      fullModuleName = moduleName
      if moduleName.split('.')[0] != 'apiservice':
        fullModuleName = 'apiservice.' + moduleName
      self._modules[moduleName] = __import__(fullModuleName, fromlist=[fromList])

  def getClassName(self, classRef):

    if ':' not in classRef:
      raise ValueError('Invalid classRef %s, expecting module:className' % classRef)
    moduleName, className = classRef.split(':')

    try:
      module = self._modules[moduleName]
    except KeyError:
      raise BaseException('Service module name not found in register : ' + moduleName)

    if not hasattr(module, className):
      raise BaseException('Service classname not found in service register : ' + className)
    
    return (module, className)

# -------------------------------------------------------------- #
# ServiceRegister
# ---------------------------------------------------------------#
class Params(object):
    def __init__(self, params):
      try:
        self.__dict__.update(params)
      except:
        raise BaseException('json decode error, bad params')

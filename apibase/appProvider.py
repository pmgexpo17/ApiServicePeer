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
      logger.info('%s, init: aready done', __name__)
      return
    logger.info('%s, init: not done', __name__)
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
    self.registry.load('/apps/home/u352425/wcauto1/temp/apiservices.json')
    self._job = {}

  # -------------------------------------------------------------- #
  # addJobGroup
  # ---------------------------------------------------------------#
  def addJobGroup(self, params, jobCount):

    director = self._job[params.id]
    module, className = self.registry.getClassName(params.service)
    jobRange = range(jobCount)

    jobs = director.listener.addJobs(jobRange)
    params.args.append(0)
    for jobNum in jobRange:
      jobId = jobs[jobNum]
      # must be an AppDelegate derivative, leveldb param is fixed by protocol
      self._job[jobId] = getattr(module, className)(self.db)
      params.args[-1] = jobNum + 1
      self.addJob(jobId, params)
      
    return jobs

  # -------------------------------------------------------------- #
  # addJob
  # ---------------------------------------------------------------#
  def addJob(self, jobId, params):

    args = [jobId] + params.args
    self.scheduler.add_job('apibase:dispatch',id=jobId,args=args,kwargs=params.kwargs)
    return jobId

  # -------------------------------------------------------------- #
  # addNewJob
  # ---------------------------------------------------------------#
  def addNewJob(self, params):

    module, className = self.registry.getClassName(params.service)
    jobId = str(uuid.uuid4())
    # must be an AppDirector derivative, leveldb and jobId params are fixed by protocol
    director = getattr(module, className)(self.db, jobId)
    # must be an AppListener derivative, leveldb param is fixed by protocol
    module, className = self.registry.getClassName(params.listener)    
    listener = getattr(module, className)(self.db)
    listener.state = director.state
    listener.addJob(jobId)
    director.listener = listener
    self._job[jobId] = director
    self.scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    self.addJob(jobId, params)
    return jobId

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, _params, jobCount=None):

    params = Params(_params)
    with self.lock:
      try:
        params.id
      except AttributeError:
        raise BaseException("required param 'id' not found")
      logger.info('job service, id : %s, %s' % (params.id, params.service))
      if params.id:
        try:
          self._job[params.id]
        except KeyError:
          raise BaseException('jobId not found in job register : ' + params.id)
        if params.type == 'delegate':
          # a live director program has dispatched a delegate
          return self.addJobGroup(params, jobCount)
        # a live director program is promoted, ie, state machine is promoted
        return self.addJob(params.id, params)
      # a director program is submitted for scheduling
      return self.addNewJob(params)    
  
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
    if not hasattr(params,'args'):
      self.args = []
    if not hasattr(params,'kwargs'):
      self.kwargs = None
    try:
      self.__dict__.update(params)
    except:
      raise BaseException('json decode error, bad params')
 

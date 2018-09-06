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
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from jobstore import LeveldbJobStore
from threading import RLock
import json
import logging
import leveldb
import os
import uuid

# ---------------------------------------------------------------------------#
# AppProvider
#
# The design intention of a smart job is to enable a group of actors to each
# run a state machine as a subprogram of an integrated super program
# The wikipedia (https://en.wikipedia.org/wiki/Actor_model) software actor 
# description says :
# In response to a message that it receives, an actor can : make local decisions, 
# create more actors, send more messages, and determine how to respond to the 
# next message received. Actors may modify their own private state, but can only 
# affect each other through messages (avoiding the need for any locks).
# ---------------------------------------------------------------------------#    
class AppProvider(object):
  _singleton = None
  _lock = RLock()
  
  @staticmethod
  def connect(dbPath):
    with AppProvider._lock:
      if AppProvider._singleton is not None:
        return AppProvider._singleton
      return AppProvider.start(dbPath)
  
  @staticmethod
  def start(dbPath):
    global logger
    logger = logging.getLogger('apscheduler')
    
    logger.info('### AppProvider is starting ... ###')
    if not os.path.isdir(dbPath):
      raise Exception("dbPath is not a directory : " + dbPath)
    appPrvdr = AppProvider()
    _leveldb = leveldb.LevelDB(dbPath)
    appPrvdr.db = _leveldb
    jobstore = LeveldbJobStore(_leveldb)
    jobstores = { 'default': jobstore } 
    scheduler = BackgroundScheduler(jobstores=jobstores)
    scheduler.start()
    appPrvdr.scheduler = scheduler
    appPrvdr.registry = ServiceRegister()
    appPrvdr._job = {}
    AppProvider._singleton = appPrvdr
    return AppProvider._singleton
    
  def __init__(self):
    self.lock = RLock()
    
  # -------------------------------------------------------------- #
  # addActorGroup
  # ---------------------------------------------------------------#
  def addActorGroup(self, params, jobRange):

    director = self._job[params.id]
    module, className = self.registry.getClassName(params.service)

    jobs = director.listener.register(jobRange)
    params.args.append(0)
    for jobNum in jobRange:
      jobId = jobs.pop(0)
      # leveldb, jobId constructor params are fixed by protocol
      self._job[jobId] = getattr(module, className)(self.db, params.id)
      params.args[-1] = jobNum
      self.runActor(params,jobId)
      # append the job to remake the original list
      jobs.append(jobId)
    return jobs

  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  def runActor(self, params, jobId):

    logger.info('appProvider.runActor job args : ' + str(params.args))
    args = [jobId] + params.args
    self.scheduler.add_job('apibase:dispatch',id=jobId,args=args,kwargs=params.kwargs,misfire_grace_time=3600)
    return jobId

  # -------------------------------------------------------------- #
  # addActor
  # ---------------------------------------------------------------#
  def addActor(self, params, jobId):

    module, className = self.registry.getClassName(params.service)
    # must be an AppDirector derivative, leveldb and jobId params are fixed by protocol
    if params.caller:
      delegate = getattr(module, className)(self.db, jobId, params.caller)
    else:
      delegate = getattr(module, className)(self.db, jobId)
    if hasattr(params, 'listener'):
      # must be an AppListener derivative, leveldb param is fixed by protocol
      module, className = self.registry.getClassName(params.listener)    
      listener = getattr(module, className)(self.db, jobId)
      listener.state = delegate.state
      delegate.listener = listener
      self.scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    self._job[jobId] = delegate
    self.runActor(params,jobId)
    return jobId

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, _params, jobId=None, jobRange=None):

    params = Params(_params)
    with self.lock:
      try:
        params.id
      except AttributeError:
        raise Exception("required param 'id' not found")
      logger.info('job service, id : %s, %s' % (params.service, params.id))
      if params.id:
        try:
          self._job[params.id]
        except KeyError:
          raise Exception('jobId not found in job register : ' + params.id)
        if params.type == 'delegate':
          # a live director is delegating an actor group
          if '-' in jobRange:
            a,b = list(map(int,jobRange.split('-')))
            _range = range(a,b)
          else:
            b = int(jobRange) + 1
            _range = range(1,b)
          return self.addActorGroup(params, _range)
        else:
          # a live director program is promoted, ie, state machine is promoted
          return self.runActor(params, params.id)
      elif not jobId:
        jobId = str(uuid.uuid4())
      # a new program, either a sync director or async delegate
      return self.addActor(params,jobId)
        
  # -------------------------------------------------------------- #
  # resolve
  # ---------------------------------------------------------------#
  def resolve(self, _params):
    
    params = Params(_params)
    module, className = self.registry.getClassName(params.service)    
    actor = getattr(module, className)(self.db)
    with self.lock:
      try:
        if params.kwargs:
          return actor(*params.args, **params.kwargs)
        return actor(*params.args)
      except Exception as ex:
        logger.error('stream generation failed : ' + str(ex))
        raise

  # -------------------------------------------------------------- #
  # evalComplete
  # ---------------------------------------------------------------#
  def evalComplete(self, actor, jobId):    
    with self.lock:
      try:
        actor.state
      except AttributeError:
        # only stateful jobs are retained
        del(self._job[jobId])
      else:
        if not actor.state.complete:
          return
        logMsg = 'director[%s] is complete, removing it now ...'
        if actor.state.failed:
          logMsg = 'director[%s] has failed, removing it now ...'
        logger.info(logMsg, jobId)
        if actor._type == 'director':
          self.removeMeta(jobId)
        if hasattr(actor, 'listener'):
          self.scheduler.remove_listener(actor.listener)
        del(self._job[jobId])

  # -------------------------------------------------------------- #
  # removeMeta
  # ---------------------------------------------------------------#
  def removeMeta(self, jobId):

    dbKey = 'PMETA|' + jobId
    self.db.Delete(dbKey)

  # -------------------------------------------------------------- #
  # register
  # ---------------------------------------------------------------#
  def register(self, serviceRef):
    self.registry.load(serviceRef)

# -------------------------------------------------------------- #
# ServiceRegister
# ---------------------------------------------------------------#
class ServiceRegister(object):

  def __init__(self):
    self._modules = {}

  def load(self, serviceRef):
    for module in serviceRef:
      self.loadModule(module['name'], module['fromList'])

  def loadModule(self, moduleName, fromList):
    try:
      self._modules[moduleName]
    except KeyError:
      _module = moduleName.split('.')[-1]
      logger.info('%s is loaded as : %s' % (moduleName, _module))
      self._modules[_module] = __import__(moduleName, fromlist=[fromList])

  def getClassName(self, classRef):

    if ':' not in classRef:
      raise ValueError('Invalid classRef %s, expecting module:className' % classRef)
    moduleName, className = classRef.split(':')

    try:
      module = self._modules[moduleName]
    except KeyError:
      raise Exception('Service module name not found in register : ' + moduleName)

    if not hasattr(module, className):
      raise Exception('Service classname not found in service register : ' + className)
    
    return (module, className)

# -------------------------------------------------------------- #
# Params
# ---------------------------------------------------------------#
class Params(object):
  def __init__(self, params):
    if not hasattr(params,'args'):
      self.args = []
    if not hasattr(params,'kwargs'):
      self.kwargs = None
    if not hasattr(params,'caller'):
      self.caller = None
    try:
      self.__dict__.update(params)
    except:
      raise Exception('params is not a dict')     

# -------------------------------------------------------------- #
# Despatch
# ---------------------------------------------------------------#
class Despatch(object):
  
  def __init__(self, appPrvdr):
    self.appPrvdr = appPrvdr
    
  def __call__(self, jobId, *args, **kwargs):

    appPrvdr = AppProvider._singleton
    
    try:
      delegate = self.appPrvdr._job[jobId]
    except KeyError:
      logger.error('jobId not found in job register : ' + jobId)
      return
  
    delegate(*argv, **kwargs)
    self.appPrvdr.evalComplete(delegate, jobId)
    

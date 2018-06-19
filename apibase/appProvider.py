# Copyright (c) 2018 Peter A McGill
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. 
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
      raise Exception("app.config['DB_PATH'] is not a directory : " + dbPath)
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
      self.addJob(params,jobId)
      
    return jobs

  # -------------------------------------------------------------- #
  # addJob
  # ---------------------------------------------------------------#
  def addJob(self, params, jobId):

    args = [jobId] + params.args
    self.scheduler.add_job('apibase:dispatch',id=jobId,args=args,kwargs=params.kwargs,misfire_grace_time=3600)
    return jobId

  # -------------------------------------------------------------- #
  # addNewJob
  # ---------------------------------------------------------------#
  def addNewJob(self, params, jobId):

    module, className = self.registry.getClassName(params.service)
    if params.type == 'delegate':
      # must be an AppDelegate derivative, leveldb and jobId params are fixed by protocol
      delegate = getattr(module, className)(self.db, jobId=jobId)
      _jobId = str(uuid.uuid4())
      self._job[_jobId] = delegate
      self.addJob(_jobId, params)
      return _jobId     
    # must be an AppDirector derivative, leveldb and jobId params are fixed by protocol
    if params.kwargs:
      director = getattr(module, className)(self.db, jobId, **params.kwargs)
    else:
      director = getattr(module, className)(self.db, jobId)
    if hasattr(params, 'listener'):
      # must be an AppListener derivative, leveldb param is fixed by protocol
      module, className = self.registry.getClassName(params.listener)    
      listener = getattr(module, className)(self.db, jobId)
      listener.state = director.state
      director.listener = listener
      self.scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    self._job[jobId] = director
    self.addJob(params,jobId)
    return jobId

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, _params, jobId=None, jobCount=None):

    params = Params(_params)
    with self.lock:
      try:
        params.id
      except AttributeError:
        raise Exception("required param 'id' not found")
      logger.info('job service, id : %s, %s' % (params.id, params.service))
      if params.id:
        try:
          self._job[params.id]
        except KeyError:
          raise Exception('jobId not found in job register : ' + params.id)
        if params.type == 'delegate':
          if params.responder == 'listener':
            # a live director program has dispatched a bound delegate
            return self.addJobGroup(params, jobCount)            
          elif params.responder == 'self':
            # a live director program has dispatched an unbound delegate
            if not jobId:
              jobId = params.id
            return self.addNewJob(params,jobId)
        else:
          # a live director program is promoted, ie, state machine is promoted
          return self.addJob(params, params.id)
      # a director program is submitted for scheduling
      return self.addNewJob(params,jobId)

  # -------------------------------------------------------------- #
  # getStreamGen
  # ---------------------------------------------------------------#
  def getStreamGen(self, params):

    with self.lock:
      try:
        delegate = self._job[params.id]
      except KeyError:
        logger.error('jobId not found in job register : ' + params.id)
      except AttributeError:
        raise Exception("required param 'id' not found")  
      try:
        streamGen = delgate.renderStream(params.dataKey)
      except Exception as ex:
        logger.error('stream generation failed : ' + params.id)
        raise
      self.evalComplete(delegate, params.id)
      return streamGen

  # -------------------------------------------------------------- #
  # evalComplete
  # ---------------------------------------------------------------#
  def evalComplete(self, delegate, jobId):
    try:
      delegate.state
    except AttributeError:
      # only stateful jobs are retained
      del(self._job[jobId])
    else:
      if not delegate.state.complete:
        return
      logMsg = 'director[%s] is complete, removing it now ...'
      if delegate.state.failed:
        logMsg = 'director[%s] has failed, removing it now ...'
      logger.info(logMsg, jobId)
      if delegate.appType == 'director':
        self.removeMeta(jobId)
      if hasattr(delegate, 'listener'):
        self.scheduler.remove_listener(delegate.listener)
      del(self._job[jobId])

  # -------------------------------------------------------------- #
  # removeMeta
  # ---------------------------------------------------------------#
  def removeMeta(self, jobId):

    dbKey = 'PMETA|' + jobId
    self.db.Delete(dbKey)

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
      raise Exception('Service module name not found in register : ' + moduleName)

    if not hasattr(module, className):
      raise Exception('Service classname not found in service register : ' + className)
    
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
    if not hasattr(params,'caller'):
      self.caller = None
    if not hasattr(params,'callee'):
      self.callee = None
    try:
      self.__dict__.update(params)
    except:
      raise Exception('params is not a dict')     

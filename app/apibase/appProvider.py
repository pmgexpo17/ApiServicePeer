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
from aiohttp import web
from apibase import ActorGroup, LeveldbHash, JobControler, JobGenerator, JobPacket
from functools import partial
from threading import RLock
import asyncio
import importlib
import logging
import leveldb
import os, subprocess, sys
import uuid

logger = logging.getLogger('apipeer.smart')

# ---------------------------------------------------------------------------#
# JobService
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
class JobService:
  
  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.cache = {}
    self.lock = RLock()

  @property
  def db(self):
    return self._leveldb

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    elif not hasattr(self, key):
      raise AttributeError(f'{key} is not a JobService attribute')
    method = getattr(self, key)
    self.__dict__[key] = method
    return method

  @staticmethod
  def make(apiBase, register):
    logger.info('### JobService is starting ... ###')
    dbPath = f'{apiBase}/database/metastore'
    if not os.path.exists(dbPath):
      subprocess.call(['mkdir','-p',dbPath])

    db = LeveldbHash(dbPath)
    JobControler.start(db, register, apiBase)
    return JobService(db)

  # -------------------------------------------------------------- #
  # create
  # ---------------------------------------------------------------#
  def create(self, jmeta):

    packet = JobPacket(jmeta)
    with self.lock:
      try:
        jobId = packet.jobId
      except AttributeError:
        raise Exception("required param jobId not found")

      try:
        controler = self.cache[jobId]
      except KeyError:
        try:
          coro = self.onCreate(jobId, packet)
          future = asyncio.ensure_future(coro)
          return {'status': 201,'jobId': jobId}
        except Exception as ex:
          return {'status': 500,'jobId': jobId,'error': str(ex)}          
      else:
        return {'status':400, 'error': f'job id {packet.jobId} exists already'}


  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, jmeta):

    packet = JobPacket(jmeta)
    with self.lock:
      try:
        jobId = packet.jobId
      except AttributeError:
        raise Exception("required param jobId not found")

      try:
        controler = self.cache[jobId]
      except KeyError:
        errmsg = f'job {jobId} does not exist, create job first'
        return {'status': 400,'jobId': jobId,'error': errmsg}

      try:
        actorId, actorName = controler[packet.actor].tell()
        if not actorId:
          actorId, actorName = controler.addActor(packet)
          logger.info(f'running new actor, {actorName}, {actorId}')
        else:
          # a live actor is promoted, ie, state machine is promoted
          logger.info(f'resuming live actor, {actorName}, {actorId}')

        controler.runTask(packet)
        return {'status': 201,'jobId': jobId,'actor': actorName,'id': actorId}
      except Exception as ex:
        logger.error(f'actor errored, {actorName}, {actorId}', exc_info=True)
        return {'status': 500,'jobId': jobId,'actor': actorName,'id': actorId,'error': str(ex)}

  # -------------------------------------------------------------- #
  # onCreate
  # ---------------------------------------------------------------#
  async def onCreate(self, jobId, packet):
    try:
      self.cache[jobId] = await JobControler.make(self._leveldb, packet)
    except asyncio.CancelledError:
      logger.error(f'{jobId}, create failed, job generator task was canceled')
    except Exception as ex:
      logger.error('job creation failed', exc_info=True)
      return {'status': 500,'jobId': jobId,'error': str(ex)}

  # -------------------------------------------------------------- #
  # delete
  # ---------------------------------------------------------------#
  def delete(self, jmeta):

    packet = JobPacket(jmeta)
    with self.lock:
      try:
        jobId = packet.jobId
      except AttributeError:
        raise Exception("required param jobId not found")

      coro = self.onDelete(jobId)
      future = asyncio.ensure_future(coro)
      return {'status': 201,'jobId': jobId}

  # -------------------------------------------------------------- #
  # onDelete
  # ---------------------------------------------------------------#
  async def onDelete(self, jobId):
    try:
      logger.info(f'job controler {jobId} will be deleted in 5 secs ...')
      await asyncio.sleep(5)
      self.cache[jobId].shutdown()
    except asyncio.CancelledError:
      logger.error(f'{jobId}, delete failed, controler task was canceled')
    except KeyError:
      logger.info(f'delete failed, job controler {jobId} is not found')
    else:
      del self.cache[jobId]
      logger.info(f'job controler {jobId} is now deleted')

  # -------------------------------------------------------------- #
  # multiTask
  # ---------------------------------------------------------------#
  def multiTask(self, jmeta, jobRange=None):

    packet = JobPacket(jmeta)
    with self.lock:
      try:
        jobId = packet.jobId
      except AttributeError:
        raise Exception("required param 'id' not found")

      try:
        controler = self.cache[jobId]
      except KeyError:
        raise Exception(f'job id not found in job register : {jobId}')

      if '-' in jobRange:
        a,b = list(map(int,jobRange.split('-')))
        jobRange = range(a,b)
      else:
        b = int(jobRange) + 1
        jobRange = range(1,b)

      try:
        caller = controler[packet.caller].serviceName
        actorGroup = ActorGroup(jobRange)
        controler.multiTask(actorGroup, packet)
        return {'status': 201,'jobId': jobId,'caller': caller,'id': actorGroup.ids}
      except Exception as ex:
        logger.error(f'actor group errored, jobId, {jobId}, caller, {caller}', exc_info=True)
        return {'status': 500,'jobId': jobId,'caller': caller,'error': str(ex)}

  # -------------------------------------------------------------- #
  # resolve
  # ---------------------------------------------------------------#
  async def resolve(self, request, jmeta):
    
    packet = JobPacket(jmeta)
    with self.lock:
      try:
        jobId = packet.jobId
      except AttributeError:
        raise Exception("required param 'id' not found")

      try:
        controler = self.cache[jobId]
      except KeyError:
        raise Exception(f'job id not found in job register : {jobId}')

      try:
        actor = controler.getActor(packet.service)        
        if packet.responseType == 'stream':
          actor.response = web.StreamResponse(status=201)
          await actor.response.prepare(request)
        return await actor(*packet.args, **packet.kwargs)
      except Exception as ex:
        logger.error('sync process failed : ' + str(ex))
        raise

  # -------------------------------------------------------------- #
  # runQuery
  # ---------------------------------------------------------------#
  def runQuery(self, jmeta):

    packet = JobPacket(jmeta)
    with self.lock:
      try:
        jobId = packet.jobId
      except AttributeError:
        raise Exception("required param 'id' not found")

      try:
        controler = self.cache[jobId]
      except KeyError:
        controler = JobControler.make(self._leveldb, packet)
    
      try:
        return web.json_response(controler.runQuery(packet), status=200)
      except Exception as ex:
        logger.error('job meta query failed, %s', packet, exc_info=True)
        return web.json_response({'error': str(ex), 'status': 400}, status=400)

  # -------------------------------------------------------------- #
  # removeMeta
  # ---------------------------------------------------------------#
  def removeMeta(self, actorId):
    dbKey = 'PMETA|' + actorId
    del self._leveldb[dbKey]

  # -------------------------------------------------------------- #
  # getLoadStatus
  # ---------------------------------------------------------------#
  def getLoadStatus(self, serviceName):
    with self.lock:
      if self.registry.isLoaded(serviceName):
        return {'status': 200,'loaded': True}
      return {'status': 200,'loaded': False}

  # -------------------------------------------------------------- #
  # loadService
  # ---------------------------------------------------------------#
  def loadService(self, serviceName, serviceRef):
    with self.lock:
      self.registry.loadModules(serviceName, serviceRef)

  # -------------------------------------------------------------- #
  # reloadModule
  # ---------------------------------------------------------------#
  def reloadModule(self, serviceName, moduleName):
    with self.lock:
      return self.registry.reloadModule(serviceName, moduleName)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  async def shutdown(self):
    [controler.shutdown() for jobId, controler in self.cache.items()]
    await JobControler.request.close()    

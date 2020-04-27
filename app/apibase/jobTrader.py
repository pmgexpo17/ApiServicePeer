__all__ = ['JobTrader']
from apibase import AbstractTxnHost, TaskError
from .jobProvider import JobProvider
from threading import RLock
import asyncio
import logging

logger = logging.getLogger('asyncio.broker')

# -------------------------------------------------------------- #
# JobTrader - managers JobDealer lifecycle, creation and deletion
# ---------------------------------------------------------------#
class JobTrader:
  def __init__(self, serverId):
    self.serverId = serverId
    self._cache = {}
    self._dcache = {}
    self._futures = set()

  def __getitem__(self, key):
    try:
      return getattr(self,key)
    except AttributeError:
      if key in self.__dict__:
        return self.__dict__[key]
      raise Exception(f'{self.name}, invalid request, {key} is not a valid api method')

  @property
  def name(self):
    return f'{self.__class__.__name__}.{self.serverId}'

  # -------------------------------------------------------------- #
  # create
  # ---------------------------------------------------------------#
  def create(self, packet):
    try:
      jobId = packet.jobId
      self._cache[jobId]
    except KeyError:
      pass
    else:
      errMsg = f'{self.name}, create failed, {jobId} job is still running'
      return [400, {'status': 400,'jobId': jobId,'error': errMsg}]

    try:
      logger.info(f'{self.name}, generating new job {jobId} ...')
      coro = self._create(jobId, packet)
      future = asyncio.ensure_future(coro)
      response = [201, {'status': 201,'method': 'create','jobId': jobId}]
    except TaskError:
      response = [500, {'status': 500,'jobId': jobId,'error': 'job generate error'}]
    except Exception as ex:
      logger.error(f'{jobId}, uncaught system error', exc_info=True)
      response = [500, {'status': 500,'jobId': jobId,'error': 'job generate error'}]
    finally:
      return response

  # -------------------------------------------------------------- #
  # install
  # ---------------------------------------------------------------#
  def install(self, jobId, dealers):
    dealer = dealers.pop(jobId)
    self._dcache[jobId] = []
    self._install(jobId, dealer)
    # install job dependency components
    for djobId, dealer in dealers.items():
      self._install(djobId, dealer)
      self._dcache[jobId].append(djobId)

  # -------------------------------------------------------------- #
  # install
  # ---------------------------------------------------------------#
  def _install(self, jobId, dealer):
    if jobId in self._cache:
      logger.warn(f'{self.name}, job generation error, created job is still active')
      return
    logger.info(f'{self.name}, installing new JobDealer, {dealer.name}, {dealer.desc}')
    self._cache[jobId] = dealer
    future = asyncio.ensure_future(dealer())
    self._futures.add(future)

  # -------------------------------------------------------------- #
  # _create
  # ---------------------------------------------------------------#
  async def _create(self, jobId, jpacket):
    try:
      jpacket.serverId = self.serverId
      dealers = await self.submit(JobProvider.run, jpacket)
      self.install(jobId, dealers)
      if self.runNow(jpacket.runMode['startTime']):
        await self._cache[jobId].perform('promote',jpacket)
    except asyncio.CancelledError:
      logger.error(f'{self.name}, job {jobId} generation was canceled')
    except TaskError:
      logger.error(f'{self.name}, job {jobId} generation errored')
    except Exception as ex:
      logger.error(f'{self.name}, uncaught system error', exc_info=True)

  # -------------------------------------------------------------- #
  # runNow
  # ---------------------------------------------------------------#
  def runNow(self, startTime):    
    return startTime['relative'] and startTime['offset'] == 0

  # -------------------------------------------------------------- #
  # runDealer
  # ---------------------------------------------------------------#
  async def runDealer(self, jobId, dealer):
    try:
      await dealer()
    except asyncio.CancelledError:
      logger.warn(f'{jobId}, job dealer task was canceled')
      dealer.shutdown()
    except Exception as ex:
      logger.error(f'{jobId}, job dealer task errored')
      raise

  # -------------------------------------------------------------- #
  # delete
  # ---------------------------------------------------------------#
  def delete(self, packet):
    try:
      jobId = packet.jobId      
      self._cache[jobId]
    except KeyError:
      errmsg = f'{self.name}, job controler {jobId} does not exist, delete aborted'
      logger.info(errmsg)
      return {'status': 400,'method': 'delete','jobId': jobId,'error': errmsg}

    try:
      coro = self._delete(jobId, packet)
      future = asyncio.ensure_future(coro)
      response = {'status': 201,'method': 'delete','jobId': jobId}
    except Exception as ex:
      logger.error(f'{self.name}, uncaught system error', exc_info=True)
      response = {'status': 500,'jobId': jobId,'error': f'job {jobId} delete error'}
    finally:
      return response

  # -------------------------------------------------------------- #
  # _delete
  # ---------------------------------------------------------------#
  async def _delete(self, jobId, packet):
    try:
      logger.info(f'{self.name}, job controler {jobId} will be deleted in 5 secs ...')
      await asyncio.sleep(5)
      await self._cache[jobId].perform('delete',packet)      
    except asyncio.CancelledError:
      logger.error(f'{self.name}, job {jobId} delete task was canceled')
    except TaskError:
      logger.error(f'{self.name}, job {jobId} delete error', exc_info=True)
    except Exception as ex:
      logger.error(f'{self.name}, uncaught system error', exc_info=True)
    finally:
      del self._cache[jobId]
      logger.info(f'{self.name}, controler {jobId} is deleted')

  # -------------------------------------------------------------- #
  # Submit
  # ---------------------------------------------------------------#
  def submit(self, func, *args, **kwargs):
    f = asyncio.Future()
    try:
      logger.info(f'{self.name}, runnable {str(func)}, will now start ...')
      result = func(*args, **kwargs)
    except Exception as ex:
      f.set_exception(ex)
    else:
        f.set_result(result)
    finally:
      return f

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self):
    pass

  # -------------------------------------------------------------- #
  # terminate
  # ---------------------------------------------------------------#
  def terminate(self, packet):
    try:
      jobId = packet.jobId      
      self._cache[jobId]
    except KeyError:
      errmsg = f'{self.name}, job controler {jobId} does not exist, terminate aborted'
      logger.info(errmsg)
      return {'status': 400,'method': 'terminate','jobId': jobId,'error': errmsg}

    try:
      coro = self._terminate(jobId, packet)
      future = asyncio.ensure_future(coro)
      response = {'status': 201,'method': 'delete','jobId': jobId}
    except Exception as ex:
      logger.error(f'{self.name}, uncaught system error', exc_info=True)
      response = {'status': 500,'jobId': jobId,'error': f'job {jobId} delete error'}
    finally:
      return response

  # -------------------------------------------------------------- #
  # _terminate
  # ---------------------------------------------------------------#
  async def _terminate(self, jobId, jpacket):
    try:
      depList = self._dcache[jobId]
      logger.info(f'{jobId}, terminate called, job dependency list : {depList}')
      for djobId in depList:
        await self._cache[djobId].perform('terminate', jpacket)  
      await self._cache[jobId].perform('terminate', jpacket)
    except asyncio.CancelledError:
      logger.error(f'{self.name}, job {jobId} generation was canceled')
    except TaskError:
      logger.error(f'{self.name}, job {jobId} generation errored')
    except Exception as ex:
      logger.error(f'{self.name}, uncaught system error', exc_info=True)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  async def shutdown(self):
    [future.cancel() for future in self._futures]
    await asyncio.sleep(1)
    [dealer.shutdown() for dealer in self._cache.values()]
  
  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    [controler.destroy() for controler in self._cache.values()]


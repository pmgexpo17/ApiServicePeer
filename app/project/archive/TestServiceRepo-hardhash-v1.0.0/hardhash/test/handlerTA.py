from apibase import LeveldbHash
from datetime import datetime
from ..handlerA import DatastoreHA
from ..context import HHBrokerContext
import asyncio
import logging
import zmq

logger = logging.getLogger('apipeer.broker')

# -------------------------------------------------------------- #
# DatastoreHT - hardhash datastore handler
# ---------------------------------------------------------------#
class DatastoreHT(DatastoreHA):

  #------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#  
  @classmethod
  def make(cls, jobId, hostAddr='tcp://127.0.0.1'):
    logger.info(f'{cls.__name__}, making broker context for job {jobId} ...')
    hhId = datetime.now().strftime('%y%m%d%H%M%S')    
    broker = HHBrokerContext.makeBroker(hhId)
    context = HHBrokerContext.make(hhId, broker.responseAddr, 'ApiConnector')
    logger.info(f'hardhash {hhId} datastore created, requestAddr : {broker.requestAddr}')
    dbKey = f'{jobId}|hardhash|makeResult'
    logger.info(f'hardhash make result dbkey : {dbKey}')
    LeveldbHash.db[dbKey] = {'hhId':hhId,'requestAddr':broker.requestAddr}
    return cls(jobId, broker, context)

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, *args, **kwargs):
    logger.info(f'###### {self.name} broker is starting ...')
    self.broker()
    return []

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self, packet):
    logger.info(f'{self.name} preparing to monitor next {packet.typeKey} lifecycle')
    return self.respond(201, packet, [])

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet):
    logger.info(f'{self.name}, starting {packet.typeKey} service {packet.taskId} ...')
    actorId, classToken = self.getActor(packet)
    coro = self.runActor(actorId, packet)
    future = asyncio.ensure_future(coro)
    return self.respond(201, packet, [future], {'actorId':actorId})

  # -------------------------------------------------------------- #
  # runActor
  # ---------------------------------------------------------------#
  async def runActor(self, actorId, packet):
    try:
      logger.info(f'@@@@ about to run {packet.taskId} actor, actorId : {actorId}')
      await self.executor.run(actorId, packet)
    except asyncio.CancelledError:
      logger.info(f'ATTN. {packet.taskKey}, service task is canceled')
      self.executor.stop(actorId)
    except Exception as ex:
      logger.error(f'{packet.taskKey}, service task errored', exc_info=True)


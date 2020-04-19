from apibase import Article, LeveldbHash, MakeArgs
from ..handlerB import MicroserviceHB
from ..microservice import Microservice
import asyncio
import importlib
import logging
import sys
import zmq

logger = logging.getLogger('apipeer.broker')

#------------------------------------------------------------------#
# MicroserviceHT
#------------------------------------------------------------------#
class MicroserviceHT(MicroserviceHB):
  def __init__(self, jobId, dsProfile):
    super().__init__(jobId, dsProfile, None)
    self._request = None

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, jobId, dspMeta):
    logger.info(f'{cls.__name__}, making MicroserviceContext for job {jobId} ...')
    dsProfile = Article(dspMeta)
    Microservice.arrange(dsProfile, 'ApiConnector')
    return cls(jobId, dsProfile)

  # -------------------------------------------------------------- #
  # runGroup
  # ---------------------------------------------------------------#
  async def runGroup(self, actorGroup, packet):
    logger.info(f'about to runGroup, {packet.actor} ...')
    result = Article({'complete':True,'failed':True,'signal':500})
    try:
      result = await self.executor.run(actorGroup, packet)
      if result.failed:
        logger.info(f'{packet.taskKey}, microservice failed, aborting ...')
    except asyncio.CancelledError:
      logger.warn(f'{packet.taskKey}, microservice task was canceled')
    except Exception as ex:
      logger.error(f'{packet.taskKey}, microservice task errored', exc_info=True)
      raise

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet):
    Microservice.prepare()
    super().promote(packet)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down ...')
    Microservice.close()

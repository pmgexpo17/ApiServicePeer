from apibase import Article, MicroserviceHA, ServiceHB
from .contextDsm import DatastreamContext
from .microserviceDsm import Microservice
import logging

logger = logging.getLogger('asyncio.broker')

# -------------------------------------------------------------- #
# DatastreamHA
# -- Hardhash datastream microservice handler - backend context
# ---------------------------------------------------------------#
class DatastreamHA(ServiceHB):

  #------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#  
  @classmethod
  def make(cls, jobId):
    logger.info(f'{cls.__name__}, making datastream context for job {jobId} ...')
    context = DatastreamContext.make(jobId)
    logger.info(f'datastream context {jobId} created')
    return cls(jobId, context)

# -------------------------------------------------------------- #
# DatastreamMsHA
# -- Hardhash datastream microservice handler - frontend context
# ---------------------------------------------------------------#
class DatastreamMsHA(MicroserviceHA):

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, jobId, peerMeta={}):
    logger.info(f'making {cls.__name__} for job {jobId} ...')
    peerNote = Article(peerMeta)
    Microservice.arrange(jobId, peerNote)
    return cls(jobId, peerNote)

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet):
    Microservice.prepare(packet.actor)
    return super().promote(packet)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down ...')
    Microservice.close()

from apibase import Article, LeveldbHash, MicroserviceHA, MicroserviceHB, ServiceHB
from datetime import datetime
from .contextHdh import HHServiceContext, HHLocalContext
from .microserviceHdh import MicroserviceA, MicroserviceB
import logging

logger = logging.getLogger('asyncio.broker')

# -------------------------------------------------------------- #
# HardhashHA 
# -- Hardhash datastore microservice handler, socket server
# ---------------------------------------------------------------#
class HardhashHA(ServiceHB):

  #------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#  
  @classmethod
  def make(cls, jobId, hhId=None):
    logger.info(f'{cls.__name__}, making hardhash broker context for job {jobId} ...')
    if not hhId:
      hhId = datetime.now().strftime('%y%m%d%H%M%S')
    else:
      logger.info(f'####### hardhash id is provided : {hhId}')      
    context = HHServiceContext.make(hhId)
    dbKey = f'{jobId}|hardhash|makeResult'
    logger.info(f'hardhash make result dbkey : {dbKey}')
    LeveldbHash.db[dbKey] = {'hhId':hhId,'contextKlass':'HHServiceContext'}
    return cls(jobId, context)

	#------------------------------------------------------------------#
	# destroy
	#------------------------------------------------------------------#
  def destroy(self):
    super().destroy()
    self.context.destroy()

	#------------------------------------------------------------------#
	# shutdown
	#------------------------------------------------------------------#
  def shutdown(self):
    super().shutdown()
    self.context.close()

# -------------------------------------------------------------- #
# HardhashMsHA
# -- Hardhash datastore microservice handler
# -- Model-HA, compliments HHLocalContext
# ---------------------------------------------------------------#
class HardhashMsHA(MicroserviceHA):

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, jobId, peerMeta):
    logger.info(f'making {cls.__name__} for job {jobId} ...')
    peerNote = Article(peerMeta)
    MicroserviceA.arrange(jobId, peerNote)
    return cls(jobId, peerNote)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down ...')
    super().shutdown()
    MicroserviceA.close()

# -------------------------------------------------------------- #
# HardhashMsHB
# -- Hardhash datastore microservice handler
# -- Model-HB, compliments HHServiceContext
# ---------------------------------------------------------------#
class HardhashMsHB(MicroserviceHB):

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, jobId, peerMeta):
    logger.info(f'making {cls.__name__} for job {jobId} ...')
    peerNote = Article(peerMeta)
    MicroserviceB.arrange(jobId, peerNote)
    return cls(jobId, peerNote)

  # -------------------------------------------------------------- #
  # promote
  # ---------------------------------------------------------------#
  def promote(self, packet):
    MicroserviceB.prepare(packet.actor)
    return super().promote(packet)

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down ...')
    super().shutdown()
    MicroserviceB.close()

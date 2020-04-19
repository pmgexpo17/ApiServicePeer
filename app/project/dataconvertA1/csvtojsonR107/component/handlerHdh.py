from apibase import Article, MicroserviceHB
from .microserviceHdh import Microservice
import logging

logger = logging.getLogger('asyncio.broker')

# -------------------------------------------------------------- #
# HandlerMsB
# -- Hardhash datastore microservice handler
# -- Model-HB, compliments HardhashContext
# ---------------------------------------------------------------#
class HandlerMsB(MicroserviceHB):

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls, jobId, hhId=None):
    logger.info(f'making {cls.__name__} for job {jobId} ...')
    Microservice.arrange(jobId,hhId)
    return cls(jobId)

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    logger.info(f'{self.name}, deleting resources ...')
    super().destroy()
    Microservice.destroy()

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down ...')
    super().shutdown()
    Microservice.close()

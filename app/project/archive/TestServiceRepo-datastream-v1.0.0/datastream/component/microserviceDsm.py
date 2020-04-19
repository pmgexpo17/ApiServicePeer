from apibase import AbstractMicroservice, AbstractSubscriptionA, LeveldbHash, MicroserviceContext, Note
from .connectorDsm import DatastreamRequest
import logging

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# Microservice
# ---------------------------------------------------------------#
class Microservice(AbstractMicroservice):
  _subscriber = None

  async def __call__(self, jobId, taskNum, *args, **kwargs):
    await self.runActor(jobId, taskNum, *args, **kwargs)
    await self._subscriber.notify(taskNum, self.name)

  @classmethod
  def arrange(cls, jobId, peerNote):
    cls._subscriber = DatastreamSubscription.make(jobId, peerNote)

  @classmethod
  def prepare(cls, actorKey):
    cls._subscriber.apply(actorKey)

  @classmethod
  def close(cls):
    cls._subscriber.close()

  @classmethod
  def subscriber(cls):
    return cls._subscriber

#----------------------------------------------------------------#
# DatastreamSubscription
#----------------------------------------------------------------#		
class DatastreamSubscription(AbstractSubscriptionA):

  @classmethod
  def make(cls, jobId, peerNote):
    context = MicroserviceContext(jobId, DatastreamRequest)
    return cls(jobId, context, peerNote)

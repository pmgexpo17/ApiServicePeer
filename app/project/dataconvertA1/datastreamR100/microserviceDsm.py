from apibase import (AbstractMicroservice, AbstractSubscriptionA, 
        MicroserviceContext, Note, ZmqDatasource)
from apitools import HardhashContext
from .connectorDsm import DatastreamRequest
import logging

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# Microservice
# ---------------------------------------------------------------#
class Microservice(AbstractMicroservice):
  _subscriber = None

  async def __call__(self, jobId, taskNum, *args, **kwargs):
    self._leveldb = HardhashContext.connector(contextId=jobId)
    await self.runActor(jobId, taskNum, *args, **kwargs)
    await self._subscriber.notify(taskNum, self.name)

  @classmethod
  def arrange(cls, jobId, peerNote):
    cls._subscriber = DatastreamSubscription.make(jobId, peerNote)
    cls.connector = cls._subscriber.connector

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
# DatastreamDatasource
#----------------------------------------------------------------#		
class DatastreamDatasource(ZmqDatasource):

  #----------------------------------------------------------------#
  # makes and returns a connector
  #----------------------------------------------------------------#
  def get(self, socktype, sockAddr, sockopt={}):
    socket = self.socket(socktype, sockopt)
    socket.connect(sockAddr)
    sockware = Note({
      'socket':socket,
      'address':sockAddr})
    return sockware

#----------------------------------------------------------------#
# DatastreamSubscription
#----------------------------------------------------------------#		
class DatastreamSubscription(AbstractSubscriptionA):

  @classmethod
  def make(cls, jobId, peerNote):
    datasource = DatastreamDatasource()
    context = MicroserviceContext(jobId, DatastreamRequest, datasource)
    return cls(jobId, context, peerNote)

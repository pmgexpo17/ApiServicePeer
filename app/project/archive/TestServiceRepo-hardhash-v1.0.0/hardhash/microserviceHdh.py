# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AbstractMicroservice, AbstractSubscriptionA, AbstractSubscriptionB, LeveldbHash, Note
from .connectorHdh import HHRequest
from .contextHdh import HHLocalContext, HHClientContext
import logging

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# MicroserviceA
# ---------------------------------------------------------------#
class MicroserviceA(AbstractMicroservice):

  @classmethod
  def arrange(cls, jobId, peerNote):
    logger.info('######################### microservice arrangement called ####################')
    cls._subscriber = HHSubscriptionA.make(jobId, peerNote)

  def __call__(self, jobId, taskNum, *args, **kwargs):
    self.runActor(jobId, taskNum, *args, **kwargs)    

#----------------------------------------------------------------#
# HHSubscriptionA
#----------------------------------------------------------------#		
class HHSubscriptionA(AbstractSubscriptionA):

  @classmethod
  def make(cls, jobId, peerNote):
    context = HHLocalContext.make()
    return cls(jobId, peerNote, context)

# -------------------------------------------------------------- #
# MicroserviceB
# ---------------------------------------------------------------#
class MicroserviceB(AbstractMicroservice):

  @classmethod
  def arrange(cls, jobId, peerNote):
    logger.info('######################### microservice arrangement called ####################')
    cls._subscriber = HHSubscriptionB.make(jobId, peerNote)

  @classmethod
  def prepare(cls, actorKey):
    cls._subscriber.apply(actorKey)

  async def __call__(self, jobId, taskNum, *args, **kwargs):
    await self.runActor(jobId, taskNum, *args, **kwargs)    
    await self._subscriber.notify(taskNum, self.name)

#----------------------------------------------------------------#
# HHSubscriptionB
#----------------------------------------------------------------#		
class HHSubscriptionB(AbstractSubscriptionB):

  @classmethod
  def make(cls, jobId, peerNote):
    metaKey = f'{peerNote.jobId}|hardhash|makeResult'
    result = Note(LeveldbHash.db[metaKey])
    context = HHClientContext(result.hhId, HHRequest)
    return cls(jobId, peerNote, context)

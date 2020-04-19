# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AbstractMicroservice, AbstractSubscriptionB, Note
from apitools import HardhashContext
import asyncio
import logging

logger = logging.getLogger('asyncio.microservice')

#----------------------------------------------------------------#
# asyncify
#----------------------------------------------------------------#		
def asyncify(func):
  def wrapper(jobId, taskNum, *args, **kwargs):
    f = asyncio.Future()
    try:
      result = func(jobId, taskNum, *args, **kwargs)
    except Exception as ex:
      f.set_exception(ex)
    else:
      f.set_result(result)
    finally:
      return f
  return wrapper

# -------------------------------------------------------------- #
# Microservice
# ---------------------------------------------------------------#
class Microservice(AbstractMicroservice):
  _subscriber = None

  @classmethod
  def arrange(cls, jobId, hhId):
    logger.debug('### xmltcsv microservice arrangement called')
    cls._subscriber = HHSubscription.make(jobId, hhId)
    cls.connector = cls._subscriber.connector

  @classmethod
  def close(cls):
    cls._subscriber.close()

  @classmethod
  def destroy(cls):
    cls._subscriber.destroy()

  @asyncify
  def __call__(self, jobId, taskNum, *args, **kwargs):
    self._hh = HardhashContext.connector(contextId=jobId)
    self.runActor(jobId, taskNum, *args, **kwargs)    

#----------------------------------------------------------------#
# HHSubscription
#----------------------------------------------------------------#		
class HHSubscription(AbstractSubscriptionB):

  @classmethod
  def make(cls, jobId, hhId):
    context = HardhashContext.get(jobId)
    subscriber = cls(jobId, context)
    return subscriber

  def connector(self, connId, owner):
    return self.context._get(connId)

  def destroy(self):
    self.context._destroy()
__all__ = [
  'AbstractMicroservice',
  'AbstractSubscriptionA',
  'AbstractSubscriptionB',
  'MicroserviceContext'
]

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import Note, LeveldbHash, TaskError
from .broker import ZmqDatasource
from .provider import ZmqConnectorCache, Connware
from .context import ApiRequest
import logging
import zmq

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# AbstractMicroservice
# ---------------------------------------------------------------#
class AbstractMicroservice:

  @property
  def name(self):
    return f'{self.__class__.__name__}.{self.taskNum}'

  def __init__(self, taskNum, actorId):
    self.taskNum = taskNum
    self.actorId = actorId

  @classmethod
  def arrange(cls):
    raise NotImplementedError(f'{cls.__name__}.arrange is an abstract method')

  @classmethod
  def close(cls):
    pass

  @classmethod
  def destroy(cls):
    pass

  @classmethod
  def prepare(cls):
    raise NotImplementedError(f'{cls.__name__}.prepare is an abstract method')

  def __call__(self, jobId, taskNum, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.__call__ is an abstract method')

#----------------------------------------------------------------#
# AbstractSubscription
#----------------------------------------------------------------#		
class AbstractSubscription:

  def __init__(self, jobId, context):
    self.jobId = jobId
    self.context = context
    self.actorKey = None

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  @classmethod
  def make(cls, jobId, peerNote):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

  # -------------------------------------------------------------- #
  # close
  # ---------------------------------------------------------------#
  def close(self):
    raise NotImplementedError(f'{self.name}.close is an abstract method')

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    raise NotImplementedError(f'{self.name}.destroy is an abstract method')

  # -------------------------------------------------------------- #
  # connector - get a client connector
  # ---------------------------------------------------------------#
  def connector(self, taskNum, owner):
    raise NotImplementedError(f'{self.name}.connector is an abstract method')

#----------------------------------------------------------------#
# AbstractSubscriptionA
#----------------------------------------------------------------#		
class AbstractSubscriptionA(AbstractSubscription):
  
  def __init__(self, jobId, context, peerNote):
    super().__init__(jobId, context)
    self.peerNote = peerNote
    self.started = []
    self.actorKey = None
    # _request is a microservice client connector to request promote (run) and restart
    self._request = ApiRequest.connector(peerNote.jobId)

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  @classmethod
  def make(cls, jobId, peerNote):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, actorKey):
    '''at session start, set the actorKey'''
    self.actorKey = actorKey

  # -------------------------------------------------------------- #
  # close
  # ---------------------------------------------------------------#
  def close(self):
    logger.info(f'{self.name}, shutting down ...')
    self.context.close()

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    self.context.destroy()

  # -------------------------------------------------------------- #
  # connector - get a client connector
  # without the asyncio eventloop, this would not be threadsafe,
  # because it is called concurrently by each microservice actor.
  # ---------------------------------------------------------------#
  async def connector(self, taskNum, owner):
    logger.info(f'### get connector {taskNum} called by {owner}')
    taskId = f'task{taskNum}'
    connector =  self.context.get(taskId)
    if connector != None:
      logger.info(f'### {self.name}, connector {connector.cid} is cached')    
      if taskNum in self.started:
        return connector
      logger.info(f'{self.name}, requesting {owner} provider service restart, taskId : {taskId} ...')
      status, response = await self.submit(taskNum, 'restart', owner)
      if status not in (200,201):
        raise TaskError(f'{self.name}, {owner} provider service restart failed : {response}')
      self.started.append(taskNum)
      logger.info(f'{self.name}, {owner} provider service {taskId} restart response : {response}')
      return connector

    # provider service creation
    logger.info(f'{self.name}, requesting {owner} provider subscription, taskId : {taskId} ...')
    # SubscriptionA promote protocol requires the ms-handler to return the service bind address
    # so that the microservice client context can connect to that address in connector creation
    status, response = await self.submit(taskNum, 'promote', owner)
    if status not in (200,201):
      raise TaskError(f'{self.name}, {owner} provider subscription failed : {response}')
    self.started.append(taskNum)
    logger.info(f'{self.name}, {owner} provider service {taskId} startup response : {response}')

    # set connware sockAddr for client connector creation
    # this sets up an exclusive microservice data channel
    sockAddr = response['sockAddr']
    connware = Connware(
      sock=[zmq.DEALER, sockAddr],
      sockopt={zmq.IDENTITY: taskId})
    connector = self.context.addConn(taskId, connware)
    logger.info(f'#### {self.name}, connector {connector.cid} added to cache')
    return connector

  # -------------------------------------------------------------- #
  # notify
  # ---------------------------------------------------------------#
  async def notify(self, taskNum, owner):
    taskId = f'task{taskNum}'
    logger.info(f'{self.name}, {owner} microservice is complete, notifying service {taskId} ...')
    connector = self.context.get(taskId)
    logger.info(f'{self.name}, connector {taskId} reference : {str(connector)}')
    if not connector:
      logger.warn(f'{self.name}, {taskId} connector was not created, notify aborted ...')
    else:
      await connector.notify(taskId, owner)
      self.started.remove(taskNum)      

  # -------------------------------------------------------------- #
  # testconn
  # ---------------------------------------------------------------#
  def testconn(self, taskNum, owner, y, payload):
    taskId = f'task{taskNum}'
    logger.info(f'{self.name}, {owner}, testing connection {taskId} ...')
    connector = self.context.get(taskId)
    if not connector:
      logger.warn(f'{self.name}, {taskId} connector was not created, notify aborted ...')
    else:
      dbKey = f'TESTING|{y}|{taskId}'
      connector[dbKey] = payload

  # -------------------------------------------------------------- #
  # submit
  # ---------------------------------------------------------------#
  async def submit(self, taskNum, request, owner):
    packet = self.peerNote.copy(merge={
      'caller': self.actorKey,
      'synchronous': True,
      'taskId': f'task{taskNum}'
    })
    await self._request.send([request, packet], owner)
    return await self._request.recv()

#----------------------------------------------------------------#
# AbstractSubscriptionB
#----------------------------------------------------------------#		
class AbstractSubscriptionB(AbstractSubscription):

  # -------------------------------------------------------------- #
  # close
  # ---------------------------------------------------------------#
  def close(self):
    logger.info(f'{self.name}, shutting down ...')

  # -------------------------------------------------------------- #
  # connector - get a client connector
  # ---------------------------------------------------------------#
  def connector(self, taskNum, owner):
    logger.info(f'### get connector {taskNum} called by {owner}')
    return self.context.get(taskNum)

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    self.context.destroy()

#------------------------------------------------------------------#
# MicroserviceContext
# -- client session context for storing client connectors
#------------------------------------------------------------------#
class MicroserviceContext(ZmqConnectorCache):
  def __init__(self, contextId, connKlass, datasource):
    super().__init__(contextId, connKlass, datasource)
    logger.info(f'{self.name}, new client connector context')    

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    logger.info(f'{self.name}, client connector context is closing ...')
    super().close()

  #----------------------------------------------------------------#
  # destroy
  #----------------------------------------------------------------#		
  def destroy(self):
    logger.info(f'{self.name}, destroy is not required')

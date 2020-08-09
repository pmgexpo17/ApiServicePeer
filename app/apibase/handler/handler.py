from apibase import Article, TaskError
import importlib
import logging
import sys, uuid

logger = logging.getLogger('asyncio.server')

# -------------------------------------------------------------- #
# AbstractHandler
# ---------------------------------------------------------------#
class AbstractHandler:
  def __init__(self, jobId):
    self.jobId = jobId

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    return getattr(self, key)

  @property
  def name(self):
    return f'{self.__class__.__name__}.{self.jobId}'

  # -------------------------------------------------------------- #
  # make
  # ---------------------------------------------------------------#
  @classmethod
  def make(cls, *args, **kwargs):
    return cls(*args, **kwargs)

  # -------------------------------------------------------------- #
  # startFw
  # ---------------------------------------------------------------#
  @classmethod
  def startFw(cls, *args, **kwargs):
    raise NotImplementedError(f'{cls.__name__}.startFw is an abstract method')

  # -------------------------------------------------------------- #
  # apply
  # ---------------------------------------------------------------#
  def apply(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.apply is an abstract method')

  # -------------------------------------------------------------- #
  # arrange
  # ---------------------------------------------------------------#
  def arrange(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.arrange is an abstract method')

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.destroy is an abstract method')

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.prepare is an abstract method')

  # -------------------------------------------------------------- #
  # request
  # ---------------------------------------------------------------#
  def request(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.request is an abstract method')

  # -------------------------------------------------------------- #
  # respond
  # ---------------------------------------------------------------#
  def respond(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.respond is an abstract method')

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.start is an abstract method')

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.stop is an abstract method')

# -------------------------------------------------------------- #
# TaskHandler
# ---------------------------------------------------------------#
class TaskHandler(AbstractHandler):
  apiBase = None

  def __init__(self, jobId):
    super().__init__(jobId)
    self.pendingFutures = set()
    self._request = None

  # -------------------------------------------------------------- #
  # startFw
  # ---------------------------------------------------------------#
  @classmethod
  def startFw(cls, apiBase):
    cls.apiBase = apiBase

  # -------------------------------------------------------------- #
  # addFuture
  # ---------------------------------------------------------------#
  def addFuture(self, future, taskKey):
    def onComplete(fut):
      try:
        self.pendingFutures.discard(fut)
        fut.result()
      except Exception as ex:
        logger.error(f'{taskKey}, task error', exc_info=True)
    self.pendingFutures.add(future)
    future.add_done_callback(onComplete)

  # -------------------------------------------------------------- #
  # arrange
  # ---------------------------------------------------------------#
  def arrange(self, connector, *args, **kwargs):
    self._request = connector

  # -------------------------------------------------------------- #
  # prepare
  # ---------------------------------------------------------------#
  def prepare(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # request
  # ---------------------------------------------------------------#
  async def request(self, method, packet, connector=None):
    if not connector:
      connector = self._request
    if method == 'terminate':
      logger.info(f'{self.name}, terminate called, connector : {connector.name}')
    await connector.send([method, packet],self.name)
    if packet['synchronous']:
      status, response = await connector.recv()
      if status not in (200,201):
        raise TaskError(f'{self.name}, {method} failed : {response}')
      return response

  # -------------------------------------------------------------- #
  # respond
  # ---------------------------------------------------------------#
  def respond(self, status, packet, mixin={}):
    if not packet.synchronous:
      return None
    response = packet.export('jobId','typeKey','caller','actor')
    if mixin:
      response.update(mixin)
    response['status'] = status
    return [status, response]

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  def shutdown(self):
    logger.info(f'{self.name}, shutting down, cancel futures ...')
    [f.cancel() for f in self.pendingFutures if not f.done()]
    self.pendingFutures.clear()    

  # -------------------------------------------------------------- #
  # start 
  # ---------------------------------------------------------------#
  def start(self, *args, **kwargs):
    pass

# -------------------------------------------------------------- #
# ActorBrief
# ---------------------------------------------------------------#
class ActorBrief(Article):
  def __init__(self, actorKey, jmeta):
    for key, value in jmeta.items():
      if isinstance(value,dict):
        jmeta[key] = Article(value)
    self.__dict__.update(jmeta)
    self.actorKey = actorKey
    self.actorId = None
    self.taskId = None
    self.actor = None
        
  def tell(self, *args):
    if not args:
      args = ['actorKey','actorId','classToken']
    return [self.__dict__[key] for key in args]

# -------------------------------------------------------------- #
# ActorCache
# ---------------------------------------------------------------#
class ActorCache(Article):  

  def __init__(self, actorKey, jmeta):
    for key, value in jmeta.items():
      if isinstance(value,dict):
        jmeta[key] = Article(value)
    self.__dict__.update(jmeta)
    self.actorKey = actorKey
    self._cache = {}

  def tell(self, taskId):
    try:
      article = self._cache[taskId]
      return (self.actorKey, article)
    except KeyError:
      return (self.actorKey, None)

  def get(self, taskId):
    return self._cache.get(taskId)

  def add(self, taskId, article):
    self._cache[taskId] = article

  def contains(self, taskId):
    return taskId in self._cache

__all__ = [
  'ApiDatasource',
  'ApiRequest',
  'ApiContext']
from apibase import Note
from .broker import ZmqDatasource
from .connector import ApiConnector
from .provider import ZmqConnProvider, ZmqConnectorCache, Connware
import logging
import zmq

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# ApiDatasource
#----------------------------------------------------------------#		
class ApiDatasource(ZmqDatasource):
  def __init__(self, broker):
    self._broker = broker

  def __getitem__(self, key):
    getkey = f'_{key}'
    if getkey in self.__dict__:
      return self.__dict__[getkey]
    return getattr(self, getkey)

  @classmethod
  def make(cls, brokerId, hostAddr, port):
    # must call ZmqDatasource.__start__ to ensure the
    # zmq.asyncio.context is available to all other descendants
    ZmqDatasource.__start__(hostAddr)
    broker = cls.makeBroker(brokerId, frontPort=port)
    return cls(broker)

  #----------------------------------------------------------------#
  # makes and returns a zmq response socket sockware item
  #----------------------------------------------------------------#
  def _respond(self, socktype, sockopt={}):
    socket = self.socket(socktype, sockopt)
    socket.connect(self._broker.responseAddr)
    sockware = Note({
      'socket':socket,
      'address':self._broker.responseAddr})
    return sockware

  #----------------------------------------------------------------#
  # makes and returns a zmq request socket sockware item
  #----------------------------------------------------------------#
  def _request(self, socktype, sockopt={}):
    socket = self.socket(socktype, sockopt)
    socket.connect(self._broker.requestAddr)
    sockware = Note({
      'socket':socket,
      'address':self._broker.requestAddr})
    return sockware

  #----------------------------------------------------------------#
  # get
  #----------------------------------------------------------------#
  def get(self, conntype, socktype, sockopt={}):
    return self[conntype](socktype,sockopt)

  #----------------------------------------------------------------#
  # run
  #----------------------------------------------------------------#
  def run(self):
    self._broker()

  #----------------------------------------------------------------#
  # shutdown
  #----------------------------------------------------------------#
  def shutdown(self):
    self._broker.shutdown()

#----------------------------------------------------------------#
# ApiContext
#----------------------------------------------------------------#		
class ApiContext(ZmqConnProvider):
  _instance = None

  def __init__(self, brokerId, datasource):
    super().__init__(brokerId, ApiConnector, datasource)

  @classmethod
  def start(cls, brokerId, datasource):
    cls._instance = cls(brokerId, datasource)    

  @property
  def title(self):
    return f'{self.name}-{self.brokerId}'

  @classmethod
  def connector(cls, connId, connKlass=None):
    connware = Connware(
      sock=['respond',zmq.DEALER],
      sockopt={zmq.IDENTITY:connId})
    return cls._instance.makeConn(connware, connKlass)

#------------------------------------------------------------------#
# ApiRequest
#------------------------------------------------------------------#
class ApiRequest(ZmqConnectorCache):
  _instance = None

  def __init__(self, brokerId, datasource):
    super().__init__(brokerId, ApiConnector, datasource)

  @classmethod
  def start(cls, brokerId, datasource):
    cls._instance = cls(brokerId, datasource)
    connware = Connware(
      sock=['request',zmq.DEALER],
      sockopt={zmq.IDENTITY:'control'})
    cls._instance.addConn('control', connware)    

  @property
  def title(self):
    return f'{self.name}-{self.brokerId}'

	#------------------------------------------------------------------#
	# connector
	#------------------------------------------------------------------#
  @classmethod
  def connector(cls, connId, connKlass=None):
    connector = cls._instance.get(connId)
    if not connector:
      connware = Connware(
        sock=['request',zmq.DEALER],
        sockopt={zmq.IDENTITY:connId})
      return cls._instance.addConn(connId, connware, connKlass)
    return connector

  #----------------------------------------------------------------#
  # stop
  #----------------------------------------------------------------#		
  @classmethod
  def stop(cls):
    logger.info('ApiRequest context is stopping ...')
    cls._instance.close()

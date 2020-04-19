from apibase import Article, ZmqDatasource, ZmqConnectorCache
from .connectorDsm import DatastreamResponse
import logging

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# DatastreamDatasource
#----------------------------------------------------------------#		
class DatastreamDatasource(ZmqDatasource):

  def get(self, socktype, sockopt={}):
    socket = self.socket(socktype, sockopt)
    port = socket.bind_to_random_port(self.hostAddr)
    sockAddr = f'{self.hostAddr}:{port}'
    sockware = Article({
      'socket':socket,
      'address':sockAddr})
    return sockware

#----------------------------------------------------------------#
# DatastreamContext
#----------------------------------------------------------------#		
class DatastreamContext(ZmqConnectorCache):

  def __init__(self, contextId, datasource):
    super().__init__(contextId, DatastreamResponse, datasource)

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#
  @classmethod
  def make(cls, jobId):
    try:
      datasource = DatastreamDatasource()
      return cls(jobId, datasource)
    except Exception as ex:
      logger.error(f'{cls.__name__}, {jobId}, context creation failed')
      raise

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    logger.info(f'{self.name} is closing ...')
    super().close()

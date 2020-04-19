__all__ = ['ApiServer']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import AbstractTxnHost, ApiContext, ApiDatasource, ApiPacket, ApiRequest, JobTrader
import asyncio
import logging
import platform

logger = logging.getLogger('asyncio.server')

# -------------------------------------------------------------- #
# ApiServer - Api Transaction Server
# ---------------------------------------------------------------#
class ApiServer(AbstractTxnHost):

  def __init__(self, connector, serverId, datasource):
    super().__init__(connector, serverId)
    self.datasource = datasource
    self.trader = JobTrader(serverId)

  #------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#  
  @classmethod
  def make(cls, hostAddr, port=5000):
    serverId = platform.node()
    datasource = ApiDatasource.make(serverId, hostAddr, port)
    ApiContext.start(serverId, datasource)
    ApiRequest.start(serverId, datasource)
    connector = ApiContext.connector('control')
    hostname = f'{cls.__name__}-{serverId}-{connector.cid}'
    logger.info(f'{hostname}, making new instance ...')    
    return cls(connector, serverId, datasource)

  # -------------------------------------------------------------- #
  # start
  # ---------------------------------------------------------------#
  def start(self):
    f = asyncio.Future()
    try:
      logger.info(f'{self.hostname} is starting ...')
      self.datasource.run()
      self.trader.start()
    except Exception as ex:
      f.set_exception(ex)
    finally:
      return f

  #----------------------------------------------------------------#
  # perform
  #----------------------------------------------------------------#
  async def perform(self, request, packet):
    try:
      response = self.trader[request](ApiPacket(packet))
      if packet['synchronous']:
        await self.send(response)
    except Exception as ex:
      logger.error(f'{self.hostname}, job transaction failed',exc_info=True)
      await self.send([404,{"error":str(ex)}])

  # -------------------------------------------------------------- #
  # terminate
  # ---------------------------------------------------------------#
  async def terminate(self):
    try:
      await asyncio.wait_for(self.shutdown(), timeout=5.0)
    except asyncio.TimeoutError:
      logger.warn(f'{self.name}, not terminated after 5 secs, destroying api context ...')
      ApiContext.destroy()

  # -------------------------------------------------------------- #
  # shutdown
  # ---------------------------------------------------------------#
  async def shutdown(self):
    logger.info(f'{self.hostname}, server is shutting down ...')
    self.datasource.shutdown()
    await self.trader.shutdown()
    ApiRequest.stop()
    logger.info(f'{self.hostname}, server shutdown is complete')    

  # -------------------------------------------------------------- #
  # destroy
  # ---------------------------------------------------------------#
  def destroy(self):
    logger.info(f'{self.hostname}, server is destroying resources ...')
    self.broker.destroy()
    self.trader.destroy()

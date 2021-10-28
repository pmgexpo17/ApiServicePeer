import asyncio

from dataclasses import dataclass, field
from .component import Article
from .connector import ConnWATC

#=================================================================#
# TxnHost - Transaction host for article exchange, and is probably
# made redundant by protocol/protocol.py 
#=================================================================#
@dataclass
class TxnHost:
  conn: ConnWATC = field(init=False, default_factory=object)
  
  @property
  def name(self):
    return f"{self.__class__.__name__}"

  #-----------------------------------------------------------------#
  # disengaged -- for descendant classes to implement specific
  # code to verify that the service is still engaged
  #-----------------------------------------------------------------#
  def disengaged(self) -> bool:
    return False

  #-----------------------------------------------------------------#
  # engaged -- for descendant classes to implement specific
  # code to verify that the service is still engaged
  #-----------------------------------------------------------------#
  def engaged(self) -> bool:
    return True

  #-----------------------------------------------------------------#
  # perform
  # -- descendants override this method to implement a service
  #-----------------------------------------------------------------#
  def perform(self):
    raise NotImplementedError(f"Attention. {self.name}.perform is still an abstract method")
  
  #-----------------------------------------------------------------#
  # receive
  #-----------------------------------------------------------------#
  async def receive(self) -> Article:
    article = await self.conn.receive()
    if article:
      return article
    return Article({"complete":0})

  #-----------------------------------------------------------------#
  # run 
  # -- listen for requests to perform
  #-----------------------------------------------------------------#
  async def run(self):
    while self.engaged():
      await self.perform()

  #-----------------------------------------------------------------#
  # start
  #-----------------------------------------------------------------#
  def start(self):
    raise NotImplementedError(f"Attention. {self.name}.start is still an abstract method")

  #-----------------------------------------------------------------#
  # started
  #-----------------------------------------------------------------#
  @property
  def started(self):
    raise NotImplementedError(f"Attention. {self.name}.started is still an abstract property")

  #-----------------------------------------------------------------#
  # shutdown
  #-----------------------------------------------------------------#
  def shutdown(self):
    raise NotImplementedError(f"Attention. {self.name}.shutdown is still an abstract method")    

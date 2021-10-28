import asyncio

from dataclasses import dataclass, field
from scraperski.component import Article, ConnWATC, Note

#=================================================================#
# Protocol - for implementing a Transaction Protocol
#=================================================================#
@dataclass
class Protocol:
  conn: ConnWATC

  #-----------------------------------------------------------------#
  # close
  #-----------------------------------------------------------------#
  def close(self):
    raise NotImplementedError(f"Attention. {self.conn.name}.close is still an abstract method")    

  #-----------------------------------------------------------------#
  # disengaged -- for descendant classes to implement specific
  # code verifying that the service is disengaged or otherwise
  #-----------------------------------------------------------------#
  def disengaged(self, article: Article) -> bool:
    return False

  #-----------------------------------------------------------------#
  # engaged -- for descendant classes to implement specific
  # code verifying that the service is still engaged
  #-----------------------------------------------------------------#
  @property
  def engaged(self) -> bool:
    return True

  #-----------------------------------------------------------------#
  # perform
  # -- descendants override this method to implement a service
  #-----------------------------------------------------------------#
  def perform(self):
    raise NotImplementedError(f"Attention. {self.conn.name}.perform is still an abstract method")
  
  #-----------------------------------------------------------------#
  # quickening -- receiving is passive because we are waiting for the
  # peer to send a response. So quickening means any non-receiving
  # behaviour. Descendents override this method for state specific
  # quickening
  #-----------------------------------------------------------------#
  def quickening(self):
    return False
  
  #-----------------------------------------------------------------#
  # receive
  #-----------------------------------------------------------------#
  async def receive(self) -> Article:
    article = await self.conn.receive()
    if article:
      return article
    return Article({"complete":0,"errored":1})

  #-----------------------------------------------------------------#
  # run 
  # -- listen for requests to perform
  #-----------------------------------------------------------------#
  async def run(self):
    while self.engaged:
      await self.perform()

  #-----------------------------------------------------------------#
  # start
  #-----------------------------------------------------------------#
  def start(self):
    raise NotImplementedError(f"Attention. {self.conn.name}.start is still an abstract method")

  #-----------------------------------------------------------------#
  # started
  #-----------------------------------------------------------------#
  @property
  def started(self):
    raise NotImplementedError(f"Attention. {self.conn.name}.started is still an abstract property")

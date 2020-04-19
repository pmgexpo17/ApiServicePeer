__all__ = [
  'AbstractDatasource',
  'AbstractConnector']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
import logging

logger = logging.getLogger('asyncio.broker')

#----------------------------------------------------------------#
# AbstractDatasource
#----------------------------------------------------------------#		
class AbstractDatasource:

  @property
  def name(self):
    return f'{self.__class__.__name__}.{self._id}'

  def close(self):
    raise NotImplementedError(f'{self.name}.close is an abstract method')

  def destroy(self):
    raise NotImplementedError(f'{self.name}.destroy is an abstract method')

  def get(self, *args, **kwargs):
    raise NotImplementedError(f'{self.name}.make is an abstract method')

  @classmethod
  def make(cls, *args, **kwargs):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

#----------------------------------------------------------------#
# AbstractConnector
#----------------------------------------------------------------#		
class AbstractConnector:

  @property
  def name(self):
    return f'{self.__class__.__name__}.{self.cid}'

  #----------------------------------------------------------------#
  # make
  #----------------------------------------------------------------#		
  @classmethod
  def make(cls, *args, **kwargs):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

  #----------------------------------------------------------------#
  # append
  #----------------------------------------------------------------#		
  def append(self):
    raise NotImplementedError(f'{self.name}.append is an abstract method')

  #----------------------------------------------------------------#
  # get
  #----------------------------------------------------------------#		
  def get(self):
    raise NotImplementedError(f'{self.name}.get is an abstract method')

  #----------------------------------------------------------------#
  # put
  #----------------------------------------------------------------#		
  def put(self):
    raise NotImplementedError(f'{self.name}.put is an abstract method')

  #----------------------------------------------------------------#
  # select
  #----------------------------------------------------------------#		
  def select(self):
    raise NotImplementedError(f'{self.name}.select is an abstract method')

  #----------------------------------------------------------------#
  # close
  #----------------------------------------------------------------#		
  def close(self):
    raise NotImplementedError(f'{self.name}.close is an abstract method')

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
from apibase import json, Note, Article
import logging

logger = logging.getLogger('asyncio.microservice')

# -------------------------------------------------------------- #
# AbstractProvider
# ---------------------------------------------------------------#
class AbstractProvider(Article):

  @property
  def name(self):
    return f'{self.__class__.__name__}'

  def __setitem__(self, key, value):
    if key in self.__dict__:
      self.__dict__[key] = value
    elif hasattr(self, str(key)): 
      setattr(self, str(key), value)

  def __getattr__(self, name):
    if name in self.__dict__:
      return self.__dict__[name]

  # -------------------------------------------------------------- #
  # __start__
  # ---------------------------------------------------------------#
  @classmethod
  def __start__(cls, metaFile):
    if not cls._nodeTree:
      with open(metaFile,'r') as fhr:
        try:
          schema = Note(json.load(fhr))
          provider = cls.make(schema)
          logger.debug(f'########## schema is loaded, maxLevel : {provider.maxLevel}')
          del provider
        except ValueError as ex:
          errmsg = 'json load error: ' + str(ex) 
          raise Exception(errmsg)

  def get(self):
    raise NotImplementedError(f'{self.name}.get is an abstract method')

	#------------------------------------------------------------------#
	# make
	#------------------------------------------------------------------#
  @classmethod
  def make(cls):
    raise NotImplementedError(f'{cls.__name__}.make is an abstract method')

	#------------------------------------------------------------------#
	# apply - set the TreeNode and NodeTree source module
	#------------------------------------------------------------------#
  @classmethod
  def apply(cls, moduleName):
    raise NotImplementedError(f'{cls.__name__}.apply is an abstract method')
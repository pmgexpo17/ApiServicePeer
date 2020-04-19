__all__ = [
  'activate',
  'Microservice',
  'TreeProvider']

from .microserviceHdh import Microservice
from .treeProvider import TreeProvider
import os

def activate():
  basePath = os.path.dirname(os.path.realpath(__file__))
  metaFile = f'{basePath}/assets/xmlXformA1.json'
  TreeProvider.__start__(metaFile)

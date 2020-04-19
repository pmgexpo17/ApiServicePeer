__all__ = [
  'activate',
  'Microservice',
  'TableProvider',
  'TreeProvider']

from .microserviceHdh import Microservice
from .treeProvider import TreeProvider, TableProviderA as TableProvider
import os

def activate():
  basePath = os.path.dirname(os.path.realpath(__file__))
  TreeProvider.__start__(f'{basePath}/assets/jsonXformA1.json')

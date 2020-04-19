__all__ = ['Application']

# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
import logging
import re

logger = logging.getLogger('apipeer.server')

#----------------------------------------------------------------#
# ApiResource
#----------------------------------------------------------------#		
class ApiResource:
  _resource = {}

#----------------------------------------------------------------#
# Application(Hashable)
#----------------------------------------------------------------#		
class Application(ApiResource):
  def __init__(self):
    self._router = Router()
    self._shutdown = []

  def __getitem__(self, key):
    if key in self._resource:
      return self._resource[key]
    return self._router[key]

  def __setitem__(self, key, value):
    self._resource[key] = value
    
  def delete(self, routeUri, handler):
    route, paramKey, params = Router.parseUri(routeUri)
    logger.debug(f'route, paramKey, params : {route}, {paramKey}, {params}')    
    # delete request, endpoint configuration
    self._router['delete'][route] = (paramKey, params, handler)

  def get(self, routeUri, handler):
    route, paramKey, params = Router.parseUri(routeUri)
    logger.debug(f'route, paramKey, params : {route}, {paramKey}, {params}')
    # get request, endpoint configuration
    self._router['get'][route] = (paramKey, params, handler)

  def post(self, routeUri, handler):
    route, paramKey, params = Router.parseUri(routeUri)
    logger.debug(f'route, paramKey, params : {route}, {paramKey}, {params}')
    # post request, endpoint configuration
    self._router['post'][route] = (paramKey, params, handler)

  def put(self, routeUri, handler):
    route, paramKey, params = Router.parseUri(routeUri)
    logger.debug(f'route, paramKey, params : {route}, {paramKey}, {params}')
    # put request, endpoint configuration
    self._router['put'][route] = (paramKey, params, handler)

  @property
  def on_shutdown(self):
    return self._shutdown

  async def shutdown(self):    
    [await shutdown(self) for shutdown in self._shutdown]  

#----------------------------------------------------------------#
# Router
#----------------------------------------------------------------#		
class Router:
  ROUTE_RE = re.compile(r'(\{[_a-zA-Z][^{}]*(?:\{[^{}]*\}[^{}]*)*\})')
  METHOD_RE = re.compile(r'\{([_a-zA-Z]+)\}')

  def __init__(self):
    self._delete = EndPoint()
    self._get = EndPoint()
    self._post = EndPoint()
    self._put = EndPoint()

  def __getitem__(self, request):
    _request = f'_{request}'
    return getattr(self, _request)

  @classmethod
  def parseUri(cls, routeUri):
    routeUri = routeUri.strip()
    assert routeUri.startswith('/')
    assert not routeUri.endswith('/')
    if not cls.ROUTE_RE.search(routeUri):
      logger.info(f'route[P0], {routeUri}')    
      return routeUri, 'P0', []

    params = []
    route = '/'
    for nextItem in cls.ROUTE_RE.split(routeUri):
      logger.debug(f'parseUri, route uri {routeUri} next item : {nextItem}')
      if cls.METHOD_RE.search(nextItem):
        nextItem = cls.METHOD_RE.match(nextItem).group(1)
        params.append(nextItem)
      elif nextItem == '/':
        continue
      if not params:
        # drop the trailing slash
        route = nextItem[:-1]
    paramKey = f'P{len(params)}'        
    logger.info(f'paramatized route[{paramKey}], {route}, {params}')    
    return route, paramKey, params

#----------------------------------------------------------------#
# EndPoint
#----------------------------------------------------------------#		
class EndPoint:
  def __init__(self):
    self._resource = {}

  def __setitem__(self, route, resourceArgs):
    paramKey, params, handler = resourceArgs
    if route not in self._resource:
      self._resource[route] = RequestResource(route)
    self._resource[route][paramKey] = (params, handler)

  def resolve(self, reqUri, packet):
    logger.info(f'resolving request uri: {reqUri}')
    for resource in self._resource.values():
      matched = resource.matches(reqUri)
      if matched:
        break
    if not matched:
      raise Exception(f'no registered routes matched the request uri {reqUri}')
    return resource.make(reqUri, packet)

  async def __call__(self, reqUrl, packet):
    request, reqUri = reqUrl.split(':')
    handler, request = self.resolve(reqUri, packet)
    logger.info(f'handler request : {request}')
    await handler[request](request)

#----------------------------------------------------------------#
# RequestResource
#----------------------------------------------------------------#		
class RequestResource:
  _handler = {}
  def __init__(self, route):
    self._route = route
    self._params = {}

  def __setitem__(self, paramKey, resourceArgs):
    params, handlerClass = resourceArgs
    self._params[paramKey] = params
    logger.info(f'{paramKey} resource handler class : {handlerClass.__name__}')
    if handlerClass not in self._handler or self._handler[paramKey].__class__ != handlerClass:
      self._handler[paramKey] = handlerClass()

  def make(self, reqUri, packet):
    if reqUri == self._route:
      return self._handler['P0'], ApiRequest(self._route, {}, packet)
    paramKey, values = self.parse(reqUri)
    params = self._params[paramKey]
    args = dict(zip(params, values))
    return self._handler[paramKey], ApiRequest(self._route, args, packet)

  def parse(self, reqUri):
    #drop the starting slash, to match the parse convention
    argsUri = reqUri.replace(self._route,'')[1:]
    values = argsUri.split('/')
    paramKey = f'P{len(values)}'
    return paramKey, values

  def matches(self, reqUri):
    reqUri = reqUri.strip()
    if not reqUri.startswith(self._route):
      return False
    if reqUri == self._route:
      logger.debug(f'simple route found : {reqUri}')
      return True
    paramKey, values = self.parse(reqUri)
    logger.debug(f'matches, resolved paramKey for request uri {reqUri} is {paramKey}')
    return paramKey in self._handler

#----------------------------------------------------------------#
# ApiRequest
#----------------------------------------------------------------#		
class ApiRequest(ApiResource):
  def __init__(self, route, reqArgs, packet):
    self._route = route
    self.match_info = reqArgs
    self.packet = packet    

  def __getitem__(self, key):
    return self._resource[key]

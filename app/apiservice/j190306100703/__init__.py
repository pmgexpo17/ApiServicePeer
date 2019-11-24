__all__ = ['promote', 'iterate']

from functools import wraps
import asyncio

promoteFw = {
  'serviceA-promote':{
    'NORMALISE_CSV':
      {'jobId': 'j191017140814', 'typeKey': 'MicroserviceA', 'actor': 'microA:normalise', 'synchronous': True, 'taskRange': 4, 'caller': {'jobId': 'j191017140814', 'actor': 'serviceA', 'typeKey': 'Service'}},
    'COMPILE_JSON':
      {'jobId': 'j191017140814', 'typeKey': 'MicroserviceA', 'actor': 'microA:compile', 'synchronous': True, 'taskRange': 1, 'caller': {'jobId': 'j191017140814', 'actor': 'serviceA', 'typeKey': 'Service'}},
    'FINAL_HANDSHAKE':
      {'jobId': 'j191017140814', 'typeKey': 'Service', 'actor': 'clientB', 'synchronous': True, 'caller': {'jobId': 'j191017140814', 'actor': 'serviceA', 'typeKey': 'Service'}},
  },
  'clientB-promote':{
    'DOWNLOAD_ZIPFILE':
      {'jobId': 'j191017140814', 'typeKey': 'MicroserviceB', 'actor': 'microB:streamreader', 'synchronous': True, 'taskRange': 1, 'caller': {'jobId': 'j191017140814', 'actor': 'clientB', 'typeKey': 'Service'}},
    'FINAL_HANDSHAKE':
      {'jobId': 'j191017140814', 'typeKey': 'Service', 'actor': 'serviceA', 'synchronous': True, 'caller': {'jobId': 'j191017140814', 'actor': 'clientB', 'typeKey': 'Service'}},
  },
}

class promote:
  def __init__(self, role):
    metaKey = f'{role}-promote'
    self.metaFw = promoteFw[metaKey]

  def __call__(self, func):
    @wraps(func)
    def wrapper(obj, *args, **kwargs):
      obj._quicken = {}
      for key, value in self.metaFw.items():
        obj._quicken[key] = value
      func(obj, *args, **kwargs)
    return wrapper

iterateFw = {
  'serviceA-iterate':{
    'NORMALISE_CSV':
      {'inTransition': True, 'hasSignal': False, 'next': 'COMPILE_JSON', 'hasNext': False, 'signalFrom': ['DatastoreA', 'MicroserviceA']},
    'COMPILE_JSON':
      {'inTransition': True, 'hasSignal': False, 'next': 'REMOVE_WORKSPACE', 'hasNext': False, 'signalFrom': ['DatastoreA', 'MicroserviceA']},
    'FINAL_HANDSHAKE':
      {'inTransition': True, 'hasSignal': True, 'next': 'REMOVE_WORKSPACE', 'hasNext': False, 'signalFrom': []},
    'REMOVE_WORKSPACE':
      {'inTransition': False, 'hasSignal': False, 'next': 'NULL', 'hasNext': False, 'complete': True, 'signalFrom': []},
  },
  'clientB-iterate':{
    'DOWNLOAD_ZIPFILE':
      {'inTransition': True, 'hasSignal': False, 'next': 'FINAL_HANDSHAKE', 'hasNext': False, 'complete': False, 'signalFrom': ['DatastreamA', 'MicroserviceB']},
    'FINAL_HANDSHAKE':
      {'inTransition': False, 'hasSignal': True, 'next': 'NULL', 'hasNext': False, 'complete': True, 'signal': 201, 'signalFrom': []},
  },
}

class iterate:
  def __init__(self, role):
    metaKey = f'{role}-iterate'
    self.metaFw = iterateFw[metaKey]

  def __call__(self, func):
    @wraps(func)
    def wrapper(obj, *args, **kwargs):
      if asyncio.iscoroutinefunction(func):
        return func(obj, *args, **kwargs)
      f = asyncio.Future()
      try:
        func(obj, *args, **kwargs)
        obj.state.__dict__.update(self.metaFw[func.__name__])
      except Exception as ex:
        f.set_exception(ex)
      else:
        f.set_result(obj.state)
      finally:
        return f
    return wrapper

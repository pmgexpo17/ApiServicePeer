__all__ = ('promote', 'iterate')

from functools import wraps

promoteFw = {
  'serviceA-promote':{
    'NORMALISE_CSV':
      [{'jobId': 'j190306100703', 'caller': 'serviceA', 'actor': 'microA:normalise', 'args': ['j190306100703'], 'kwargs': {'hhGroupSize': 4}}, 'http://localhost:5000/api/v1/multi/4'],
    'COMPILE_JSON':
      [{'jobId': 'j190306100703', 'caller': 'serviceA', 'actor': 'microA:compile', 'args': ['j190306100703']}, 'http://localhost:5000/api/v1/multi/1'],
    'COMPOSE_JSFILE':
      [{'jobId': 'j190306100703', 'caller': 'serviceA', 'actor': 'microA:compose', 'args': ['j190306100703']}, 'http://localhost:5000/api/v1/multi/1'],
    'FINAL_HANDSHAKE':
      [{'jobId': 'j190306100703', 'caller': 'serviceA', 'actor': 'clientB', 'args': ['j190306100703']}, 'http://localhost:5000/api/v1/smart'],
  },
  'clientB-promote':{
    'DOWNLOAD_ZIPFILE':
      [{'jobId': 'j190306100703', 'caller': 'clientB', 'actor': 'serviceA', 'args': ['j190306100703']}, 'http://localhost:5000/api/v1/smart'],
  },
}

class promote:
  def __init__(self, role):
    metaKey = f'{role}-promote'
    self.metaFw = promoteFw[metaKey]

  def __call__(self, func):
    @wraps(func)
    def wrapper(obj, *args, **kwargs):
      ''' this decorator applies framework compiled state metadata
          to enable state machine iteration according an original 
          UML state diagram'''
      obj._quicken = {}
      obj._quicken.update(self.metaFw)
      return func(obj, *args, **kwargs)
    return wrapper

iterateFw = {
  'serviceA-iterate':{
    'EVAL_XFORM_META':
      {'inTransition': False, 'hasSignal': False, 'next': 'NORMALISE_CSV', 'hasNext': True},
    'NORMALISE_CSV':
      {'inTransition': True, 'hasSignal': False, 'next': 'COMPILE_JSON', 'hasNext': False},
    'COMPILE_JSON':
      {'inTransition': True, 'hasSignal': False, 'next': 'COMPOSE_JSFILE', 'hasNext': False},
    'COMPOSE_JSFILE':
      {'inTransition': True, 'hasSignal': False, 'next': 'FINAL_HANDSHAKE', 'hasNext': False},
    'FINAL_HANDSHAKE':
      {'inTransition': True, 'hasSignal': False, 'next': 'REMOVE_WORKSPACE', 'hasNext': False},
    'REMOVE_WORKSPACE':
      {'inTransition': False, 'hasSignal': False, 'next': 'NULL', 'hasNext': False, 'complete': True},
  },
  'clientB-iterate':{
    'DOWNLOAD_ZIPFILE':
      {'inTransition': False, 'hasSignal': True, 'next': 'NULL', 'hasNext': False, 'complete': True},
  },
}

class iterate:
  def __init__(self, role):
    metaKey = f'{role}-iterate'
    self.metaFw = iterateFw[metaKey]

  def __call__(self, func):
    @wraps(func)
    def wrapper(obj, *args, **kwargs):
      ''' this decorator applies framework compiled state metadata
          to enable state machine iteration according an original
          UML state diagram'''
      obj.state.__dict__.update(self.metaFw[func.__name__])
      func(obj, *args, **kwargs)
      return obj.state
    return wrapper
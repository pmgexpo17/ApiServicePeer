# The MIT License
#
# Copyright (c) 2018 Peter A McGill
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
from abc import ABCMeta, abstractmethod
from threading import RLock
from subprocess import Popen, PIPE, call as subcall
from requests import HTTPError, ConnectionError, RequestException
from urllib3.exceptions import NewConnectionError, ConnectTimeoutError, MaxRetryError
import simplejson as json
import logging
import os
import requests

logger = logging.getLogger('apipeer.smart')

# -------------------------------------------------------------- #
# SysCmdUnit
# ---------------------------------------------------------------#
class SysCmdUnit:

  # ------------------------------------------------------------ #
  # sysCmd
  # - use for os commands so return code is handled correctly
  # -------------------------------------------------------------#
  def sysCmd(self, sysArgs, stdin=None, stdout=None, cwd=None, shell=False):

    try:
      scriptname = self.__class__.__name__
      if stdout:
        return subcall(sysArgs, stdin=stdin, stdout=stdout, stderr=stdout, cwd=cwd, shell=shell)  
      else:
        return subcall(sysArgs, stdin=stdin, cwd=cwd, shell=shell)
    except OSError as ex:
      errmsg = '%s syscmd failed, args : %s\nError : %s' % (scriptname, str(sysArgs), str(ex))
      raise Exception(errmsg)

  # ------------------------------------------------------------ #
  # runProcess
  # -------------------------------------------------------------#
  def runProcess(self, sysArgs, cwd=None, stdin=None, stdout=None):
    
    try:
      scriptname = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
      if stdin:
        prcss = Popen(sysArgs,stdin=PIPE,cwd=cwd)
        prcss.communicate(stdin)
        return
      elif stdout:
        prcss = Popen(sysArgs,stdout=stdout,stderr=stdout,cwd=cwd)
        prcss.communicate()
        return
      prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE,cwd=cwd)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        errmsg = '%s syscmd failed, args : %s\nError : %s' % (scriptname, str(sysArgs), str(stderr))
        raise Exception(errmsg)
      return stdout
    except OSError as ex:
      errmsg = '%s syscmd failed, args : %s\nError : %s' % (scriptname, str(sysArgs), str(ex))
      raise Exception(errmsg)

# -------------------------------------------------------------- #
# AppDirector
# ---------------------------------------------------------------#
class AppDirector(SysCmdUnit):
  __metaclass__ = ABCMeta

  def __init__(self, leveldb, actorId):
    self.lock = RLock()
    self._leveldb = leveldb
    self.actorId = actorId
    self.state = AppState(actorId)
    self.resolve = None

  @property
  def id(self):
    return self.actorId

  @property
  def name(self):
    return self.__class__.__name__

  def tell(self):
    return self.actorId, self.__class__.__name__

  # ------------------------------------------------------------ #
  # thread / process callable method
  # -------------------------------------------------------------#
  def __call__(self, *args, **kwargs):
    with self.lock:
      try:
        self.apply(*args, **kwargs)
      except Exception as ex:
        logger.error('actor error',exc_info=True)

  # ------------------------------------------------------------ #
  # apply
  # -------------------------------------------------------------#
  def apply(self, *args, **kwargs):
    if self.state.status == 'STOPPED':
      try:
        self.state.status = 'STARTING'
        self._start(*args, **kwargs)
        self.state.status = 'STARTED'
      except Exception as ex:
        self.state.failed = True
        self.state.complete = True
        self.onError(ex)
    elif self.state.status == 'STARTED':
      try:
        self.runApp(*args, **kwargs)
      except Exception as ex:
        self.state.failed = True
        self.state.complete = True
        self.onError(ex)

  # -------------------------------------------------------------- #
  # runApp
  # ---------------------------------------------------------------#
  def runApp(self, *args, signal=None, **kwargs):

    try:      
      state = self.state
      if state.inTransition and signal is not None:
        logger.info('%s received signal : %d' % (self.__class__.__name__, signal))
        state = self.advance(signal)
        if state.inTransition:
          logger.info('transition incomplete, multiple signals required ...')
          # in this case multiple signals are required to resolve the state transition
          # until all signals are received state.hasNext must remain False
          return
        logger.info('state transition resolved by signal : ' + str(signal))
      while state.hasNext: # complete?
        logger.info('%s resolving state : %s' % (self.__class__.__name__,state.current))
        self.state = state = self.resolve[state.current]()
        if state.inTransition and state.hasSignal: 
          logger.info('quicken state transition %s ... ' % state.current)
          self.quicken()
          break
        state = self.advance()
      if state.complete and state.hasSignal:
        self.quicken()
    except Exception as ex:
      self.state.complete = True
      self.state.failed = True
      self.onError(ex)

  # -------------------------------------------------------------- #
  # _start
  # -------------------------------------------------------------- #
  @abstractmethod
  def _start(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # advance
  # -------------------------------------------------------------- #
  @abstractmethod
  def advance(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
  @abstractmethod  
  def onError(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  @abstractmethod
  def quicken(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # db
  # ---------------------------------------------------------------#
  @property
  def db(self):
    return self._leveldb

# -------------------------------------------------------------- #
# AppState
# ---------------------------------------------------------------#
class AppState:

  def __init__(self, actorId=None):
    self.actorId = actorId
    self.complete = False
    self.current = 'INIT'
    self.failed = False
    self.hasNext = False
    self.hasOutput = False
    self.hasSignal = False
    self.inTransition = False
    self.lock = RLock()
    self.next = 'INIT'
    self.signalFrom = []
    self.status = 'STOPPED'

  def __getitem__(self, key):
    if key in self.__dict__:
      return self.__dict__[key]
    else:
      raise KeyError(f'{key} is not found')

  def onError(self):
    self.complete = True
    self.failed = True
    self.status = 'FAILED'

# -------------------------------------------------------------- #
# AppResolvar
# ---------------------------------------------------------------#
class AppResolvar(SysCmdUnit):

  @property
  def name(self):
    return self.__class__.__name__

  def __getitem__(self, key):
    # lazy load method into __dict__
    if key in self.__dict__:
      return self.__dict__[key]
    elif not hasattr(self, key):
      raise AttributeError(f'{key} is not an attribute of {self.__class__.__name__}')
    method = getattr(self, key)
    self.__dict__[key] = method
    return method

  def __setitem__(self, key, value):
      self.__dict__[key] = value

  def __delitem__(self, key):
      del self.__dict__[key]

  def __contains__(self, key):
      return key in self.__dict__

  def __len__(self):
      return len(self.__dict__)

  def __repr__(self):
      return repr(self.__dict__)

# -------------------------------------------------------------- #
# AppListener
# ---------------------------------------------------------------#
class AppListener:
  __metaclass__ = ABCMeta

  def __init__(self, leveldb, caller):
    self._leveldb = leveldb
    self.caller = caller

  # -------------------------------------------------------------- #
  # addJobs - add a list of live job ids
  # ---------------------------------------------------------------#
  @abstractmethod
  def register(self, *args, **kwargs):
    pass

# -------------------------------------------------------------- #
# ApiConnectError
# ---------------------------------------------------------------#
class ApiConnectError(Exception):
  pass

# -------------------------------------------------------------- #
# apiStatus
# ---------------------------------------------------------------#
def apiStatus(func):
  def wrapper(self, *args, **kwargs):
    try:
      return func(self, *args, **kwargs)
    except HTTPError as ex:
      raise Exception('api request failed\nHttp error : ' + str(ex))
    except (ConnectionError, NewConnectionError, ConnectTimeoutError, MaxRetryError) as ex:
      if 'Errno 111' in ex.__repr__():
        raise ApiConnectError('api request failed, api host is not running\nConnection error : ' + str(ex))
      else:
        raise Exception('api request failed\nConnection error: ' + str(ex))
    except RequestException as ex:
      raise Exception('api request failed\nError : ' + str(ex))
  return wrapper

# -------------------------------------------------------------- #
# ApiRequest
# ---------------------------------------------------------------#
class ApiRequest:
  def __init__(self):
    self.conn = requests.Session()

  def close(self):
    self.conn.close()
    
  @apiStatus
  def delete(self, *args, **kwargs):
    return self.conn.delete(*args, **kwargs)

  @apiStatus
  def get(self, *args, **kwargs):
    return self.conn.get(*args, **kwargs)

  @apiStatus
  def post(self, *args, **kwargs):
    return self.conn.post(*args, **kwargs)

  @apiStatus
  def put(self, *args, **kwargs):
    return self.conn.put(*args, **kwargs)

# -------------------------------------------------------------- #
# MetaReader
# ---------------------------------------------------------------#
class MetaReader:
  __metaclass__ = ABCMeta

  def __getitem__(self, key):
      return self.__dict__[key]

  def __setitem__(self, key, value):
      self.__dict__[key] = value

  def __delitem__(self, key):
      del self.__dict__[key]

  def __contains__(self, key):
      return key in self.__dict__

  def __len__(self):
      return len(self.__dict__)

  def __repr__(self):
      return repr(self.__dict__)
     
  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  @abstractmethod
  def getProgramMeta(self):
    pass

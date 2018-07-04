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
import json
import logging
import os
import re

logger = logging.getLogger('apscheduler')

# -------------------------------------------------------------- #
# SysCmdUnit
# ---------------------------------------------------------------#
class SysCmdUnit(object):

  # ------------------------------------------------------------ #
  # sysCmd
  # -------------------------------------------------------------#
  def sysCmd(self, sysArgs, stdout=None, cwd=None):

    try:
      scriptname = self.__class__.__name__
      return subcall(sysArgs, stdout=stdout, cwd=cwd)
    except OSError as ex:
      errmsg = '%s syscmd failed : %s' % (scriptname, str(ex))
      raise Exception(errmsg)

  # ------------------------------------------------------------ #
  # runProcess
  # -------------------------------------------------------------#
  def runProcess(self, sysArgs, cwd=None, stdin=None, stdout=None):
    
    try:
      scriptname = self.__class__.__name__
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
        if not stderr:
          stderr = ' '.join(sysArgs)
        errmsg = '%s syscmd failed : %s' % (scriptname, stderr)
        raise Exception(errmsg)
      return stdout
    except OSError as ex:
      errmsg = '%s syscmd failed : %s' % (scriptname, str(ex))
      raise Exception(errmsg)

# -------------------------------------------------------------- #
# AppDirector
# ---------------------------------------------------------------#
class AppDirector(SysCmdUnit):
  __metaclass__ = ABCMeta

  def __init__(self, leveldb, jobId):
    self._leveldb = leveldb
    self.jobId = jobId
    self.state = AppState(jobId)
    self.resolve = None
    self.hasSignal = False

  # ------------------------------------------------------------ #
  # generic actor method
  # -------------------------------------------------------------#
  def __call__(self, *argv, **kwargs):
    with self.state.lock:
      if self.state.status == 'STOPPED':
        try:
          self._start()
          self.state.status = 'STARTED'
        except Exception as ex:
          self.state.complete = True
          self.state.failed = True
          logger.error('_start failed : ' + str(ex))
          return
      try:
        self.runApp(*argv, **kwargs)
      except Exception as ex:
        self.state.complete = True
        self.state.failed = True        
        logger.error('runApp failed : ' + str(ex))

  # -------------------------------------------------------------- #
  # runApp
  # ---------------------------------------------------------------#
  def runApp(self, signal=None):

    try:      
      state = self.state
      if state.inTransition and signal is not None:
        logger.info('received signal : %d' % signal)
        state = self.advance(signal)
        if state.inTransition:
          # multiple signals required for successful state transition 
          return 
        logger.info('state transition resolved by signal : ' + str(signal))
      while state.hasNext: # complete?
        logger.info('resolving state : ' + state.current)
        state = self.resolve[state.current]()
        if state.inTransition:
          logger.info('in transition to next state %s , so quicken ...' % state.next)
          self.quicken()
          break  
        state = self.advance()
      if self.state.hasSignal:
        self.quicken()
    except Exception as ex:
      #self.mailer[state.current]('ERROR')
      self.onError(str(ex))
      self.state.complete = True
      self.state.failed = True
      logger.error('director failed : ' + str(ex))

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
# AppState
# ---------------------------------------------------------------#
class AppState(object):

  def __init__(self, jobId=None):
    self.complete = False
    self.current = 'INIT'
    self.failed = False
    self.inTransition = False
    self.jobId = jobId    
    self.hasNext = False
    self.hasSignal = False
    self.lock = RLock()
    self.next = 'INIT'
    self.status = 'STOPPED'
    self.transition = 'NA'
    
# -------------------------------------------------------------- #
# AppResolveUnit
# ---------------------------------------------------------------#
class AppResolveUnit(SysCmdUnit):

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
# AppListener
# ---------------------------------------------------------------#
class AppListener(object):
  __metaclass__ = ABCMeta

  def __init__(self, leveldb, jobId):
    self._leveldb = leveldb
    self.jobId = jobId

  # -------------------------------------------------------------- #
  # addJobs - add a list of live job ids
  # ---------------------------------------------------------------#
  @abstractmethod
  def register(self, *args, **kwargs):
    pass

# -------------------------------------------------------------- #
# MetaReader
# ---------------------------------------------------------------#
class MetaReader():
  __metaclass__ = ABCMeta

  def __init__(self, leveldb):
    self._leveldb = leveldb
    self.pmeta = None

  # -------------------------------------------------------------- #
  # restoreMeta
  # ---------------------------------------------------------------#
  def restoreMeta(self, dbKey):
    try:
      pmetadoc = self._leveldb.Get(dbKey)
    except KeyError:
      raise Exception('EEOWW! pmeta json document not found : ' + dbKey)
    return json.loads(pmetadoc)
        
  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  @abstractmethod
  def getProgramMeta(self):
    pass

# -------------------------------------------------------------- #
# SasScriptPrvdr
# ---------------------------------------------------------------#
class SasScriptPrvdr(MetaReader):

  # -------------------------------------------------------------- #
  # compileScript
  # ---------------------------------------------------------------#
  def compileScript(self, sasfile, incItems=None):
    logger.debug('progLib : ' + self.pmeta['progLib'])
    tmpltName = '%s/%s' % (self.pmeta['assetLib'], sasfile)
    sasFile = '%s/%s' % (self.pmeta['progLib'], sasfile)
    fhr = open(tmpltName,'r')
    _puttext = '  %let {} = {};\n'
    with open(sasFile,'w') as fhw :
      for line in fhr:
        if re.search(r'<insert>', line):
          self.putMetaItems(_puttext, fhw, incItems)
        else:
          fhw.write(line)
    fhr.close()

  # -------------------------------------------------------------- #
  # putMetaItems
  # ---------------------------------------------------------------#                                          
  def putMetaItems(self, puttext, fhw, incItems):
    for itemKey, metaItem in self.pmeta.items():
      if not incItems or itemKey in incItems:
        fhw.write(puttext.format(itemKey, metaItem))

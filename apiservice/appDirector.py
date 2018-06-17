# Copyright (c) 2018 Peter A McGill
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. 
#
from abc import ABCMeta, abstractmethod
from threading import RLock
from subprocess import Popen, PIPE, call as subcall
import logging
import os

logger = logging.getLogger('apscheduler')

# -------------------------------------------------------------- #
# AppDelegate
# ---------------------------------------------------------------#
class AppDelegate(object):

  def __init__(self, leveldb, jobId=None):
    self._leveldb = leveldb
    self.jobId = jobId
    self.appType = 'delegate'

  # ------------------------------------------------------------ #
  # sysCmd
  # -------------------------------------------------------------#
  def sysCmd(self, sysArgs, cwd=None):

    try:
      scriptname = self.__class__.__name__
      if cwd:
        return subcall(sysArgs, cwd=cwd)
      else:
        return subcall(sysArgs)
    except OSError as ex:
      errmsg = '%s syscmd failed : %s' % (scriptname, str(ex))
      logger.error(errmsg)
      raise Exception(errmsg)

  # ------------------------------------------------------------ #
  # runProcess
  # -------------------------------------------------------------#
  def runProcess(self, sysArgs, cwd=None):
    
    try:
      scriptname = self.__class__.__name__
      if cwd:
        prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE,cwd=cwd)
      else:
        prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
      if prcss.returncode:
        if not stderr:
          stderr = ' '.join(sysArgs)
        errmsg = '%s syscmd failed : %s' % (scriptname, stderr)
        logger.error(errmsg)
        raise Exception(errmsg)
      return stdout
    except OSError as ex:
      errmsg = '%s syscmd failed : %s' % (scriptname, str(ex))
      logger.error(errmsg)
      raise Exception(errmsg)

# -------------------------------------------------------------- #
# AppDirector
# ---------------------------------------------------------------#
class AppDirector(AppDelegate):
  __metaclass__ = ABCMeta

  def __init__(self, leveldb, jobId):
    super(AppDirector,self).__init__(leveldb, jobId=jobId)
    self.state = AppState(jobId)
    self.resolve = None
        
  # run wrkcmp emulation by apScheduler
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
        logger.info('next state : ' + self.state.current)
      if state.complete:
        self.onComplete(self)
    except Exception as ex:
      #self.mailer[state.current]('ERROR')
      self.onError(str(ex))
      self.state.complete = True
      self.state.failed = True
      logger.error('runApp failed : ' + str(ex))

  # -------------------------------------------------------------- #
  # advance
  # -------------------------------------------------------------- #
  @abstractmethod
  def advance(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # onComplete
  # ---------------------------------------------------------------#
  @abstractmethod
  def onComplete(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # onError
  # ---------------------------------------------------------------#
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
    self.jobId = jobId
    self.current = 'INIT'
    self.next = 'INIT'
    self.transition = 'NA'
    self.inTransition = False
    self.hasNext = False
    self.complete = False
    self.failed = True
    self.lock = RLock()
    self.status = 'STOPPED'

# -------------------------------------------------------------- #
# AppResolveUnit
# ---------------------------------------------------------------#
class AppResolveUnit(object):
  def __init__(self):
    pass
    # setup resolve state mapping here eg -->
    # self.__dict__['INIT'] = self.INIT

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

  def __init__(self, leveldb):
    self._leveldb = leveldb

  # -------------------------------------------------------------- #
  # addJob - add a live job id
  # ---------------------------------------------------------------#
  @abstractmethod
  def addJob(self, *args, **kwargs):
    pass

  # -------------------------------------------------------------- #
  # addJobs - add a list of live job ids
  # ---------------------------------------------------------------#
  @abstractmethod
  def addJobs(self, *args, **kwargs):
    pass

# -------------------------------------------------------------- #
# StreamPrvdr
# ---------------------------------------------------------------#
class StreamPrvdr(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def renderStream(self, *args, **kwargs):
    pass

# -------------------------------------------------------------- #
# SasScriptPrvdr
# ---------------------------------------------------------------#
class SasScriptPrvdr(AppDelegate):
  __metaclass__ = ABCMeta

  # -------------------------------------------------------------- #
  # getProgramMeta
  # ---------------------------------------------------------------#
  @abstractmethod
  def getProgramMeta(self):
    pass

  # -------------------------------------------------------------- #
  # compileScript
  # ---------------------------------------------------------------#
  def compileScript(self, sasfile, incItems=None):
    logger.debug('progLib : ' + self.pmeta['progLib'])
    tmpltName = '%s/%s' % (self.pmeta['tmpltLib'], sasfile)
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


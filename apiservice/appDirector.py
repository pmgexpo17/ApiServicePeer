from threading import RLock
from subprocess import Popen, PIPE
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

  # ------------------------------------------------------------ #
  # runProcess
  # -------------------------------------------------------------#
  def runProcess(self, sysArgs, returnRc=False, cwd=None):
    
    try:
      scriptname = self.__class__.__name__
      if cwd:
        prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE,cwd=cwd)
      else:
        prcss = Popen(sysArgs,stdout=PIPE,stderr=PIPE)
      (stdout, stderr) = prcss.communicate()
      if returnRc:
        return prcss.returncode
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

  def __init__(self, leveldb, jobId):
    super(AppDirector,self).__init__(leveldb, jobId=jobId)
    self.state = AppState(jobId)
    self.resolve = None
    self.runMode = 'INIT'
        
  # run wrkcmp emulation by apScheduler
  def __call__(self, *argv, **kwargs):

    with self.state.lock:
      if self.runMode == 'INIT':
        try:
          self._start()
          self.runMode = 'STARTED'
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
    except Exception as ex:
      self.mailer[state.current]('ERROR')
      self.state.complete = True
      self.state.failed = True
      logger.error('runApp failed : ' + str(ex))

  # -------------------------------------------------------------- #
  # advance
  # -------------------------------------------------------------- #
  def advance(self, signal=None):
    pass

  # -------------------------------------------------------------- #
  # quicken
  # ---------------------------------------------------------------#
  def quicken(self):
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

  def __init__(self, leveldb):
    self._leveldb = leveldb

# -------------------------------------------------------------- #
# ApiException
# ---------------------------------------------------------------#
class ApiException(Exception):
  def __init__(self, message):
    self.message = message

  def __str__(self):
    return self.message

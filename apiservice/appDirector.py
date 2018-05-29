import logging
from threading import RLock

logger = logging.getLogger('apscheduler')
# -------------------------------------------------------------- #
# AppDirector
# ---------------------------------------------------------------#
class AppDirector(object):

  def __init__(self):
    self.state = AppState()
    self.resolve = None
        
  # run wrkcmp emulation by apScheduler
  def __call__(self, *argv, **kwargs):

    with self.state.lock:
      self.runApp(*argv, **kwargs)

  # -------------------------------------------------------------- #
  # runApp
  # ---------------------------------------------------------------#
  def runApp(self, signal=None):

    try:
      state = self.state
      if state.inTransition and signal:
        logger.info('received signal : ' + str(signal))
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
      raise BaseException('runApp failed : ' + str(ex))

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

  def __init__(self):
    self.current = 'INIT'
    self.next = 'INIT'
    self.transition = 'NA'
    self.inTransition = False
    self.hasNext = False
    self.complete = False
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
# AppDelegate
# ---------------------------------------------------------------#
class AppDelegate(object):

  def __init__(self, leveldb):
    self._leveldb = leveldb

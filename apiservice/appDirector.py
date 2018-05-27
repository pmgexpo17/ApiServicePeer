from threading import RLock

# -------------------------------------------------------------- #
# AppDirector
# ---------------------------------------------------------------#
class AppDirector(object):

  def __init__(self):
    self.state = AppState()
    self.resolve = None
    self.advance = None
    self.quicken = None

  # run wrkcmp emulation by apScheduler
  def __call__(self, *argv, **kwargs):

    signal = None
    if hasattr(kwargs, 'signal'):
      signal = kwargs['signal']
    with self.state.lock:
      self.runApp(signal, argv)

  def runApp(self, signal, *argv):

    try:
      state = self.state
      if state.inTransition and signal:
        state = self.advance(signal)
        if state.inTransition:
          # multiple signals required for successful state transition 
          return 
      while state.hasNext: # complete?
        state = self.resolve[state.current]
        if state.inTransition:
          # state
          self.quicken()
          break  
        state = self.advance()
    except Exception:
      raise BaseException('jobId not found in job register : ' + params.jobId)

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

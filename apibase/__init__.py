from appProvider import AppProvider

appPrvdr = AppProvider()
appPrvdr.init('/home/workspace/leveldb/devdb1')

def runJob(jobId, *argv):
  logger = logging.getLogger('apscheduler')
  try:
    delegate = appPrvdr._job[jobId]
  except KeyError:
    logger.error('jobId not found in job register : ' + jobId)
    return

  delegate(argv)
  if delegate.state.complete:
    with appPrvdr.lock:
      if hasattr(delegate, 'listener'):
        appPrvdr.scheduler.remove_listener(delegate.listener)
      del(appPrvdr._job[jobId])

  

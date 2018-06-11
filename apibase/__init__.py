from appProvider import AppProvider
import logging

appPrvdr = AppProvider()
appPrvdr.init('/apps/home/u352425/emltnApi')
#appPrvdr.init('/home/workspace/leveldb/devdb1')

def dispatch(jobId, *argv, **kwargs):
  logger = logging.getLogger('apscheduler')
  try:
    delegate = appPrvdr._job[jobId]
  except KeyError:
    logger.error('jobId not found in job register : ' + jobId)
    return
  
  delegate(*argv, **kwargs)
  try:
    delegate.state
    delegate.listener
  except AttributeError:
    # only stateful jobs are retained
    try:
      if delegate.removeMeta:
        removeMeta(jobId)
    except AttributeError:
      pass
    with appPrvdr.lock:
      del(appPrvdr._job[jobId])
    return
  if delegate.state.complete:
    logtxt = 'director[%s] is complete, removing it now ...'
    if delegate.runMode == 'FAILED':
      logtxt = 'director[%s] has failed, removing it now ...'
    logger.info(logtxt, delegate.state.jobId)
    removeMeta(jobId)
    with appPrvdr.lock:
      if hasattr(delegate, 'listener'):
        appPrvdr.scheduler.remove_listener(delegate.listener)
      del(appPrvdr._job[jobId])

def removeMeta(jobId):

  dbKey = 'PMETA|' + jobId
  appPrvdr.db.Delete(dbKey)

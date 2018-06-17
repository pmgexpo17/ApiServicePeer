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
import leveldb
import datetime

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
  import cPickle as pickle
except ImportError:  # pragma: nocover
  import pickle

 
class LeveldbJobStore(BaseJobStore):
  """
    Stores jobs in a leveldb database.
    :param int dbPath: the leveldb instance system path
    :param int pickleProtocol: pickle protocol level to use (for serialization), defaults to the
        highest available
  """
  def __init__(self, levelDb, pickleProtocol=pickle.HIGHEST_PROTOCOL):
    super(LeveldbJobStore, self).__init__()

    self.db = levelDb
    # key store prefix for data segments
    self.jobKey = 'JOB|'
    self.cronKey = 'CRON|'
    self.xrefKey = 'JOBXREF|'
    self.pickleProtocol = pickleProtocol

  # get_job_key - create or retrieve the job_key
  def get_job_key(self, job_id=None):
    if not job_id:
      return self.jobKey + self.get_timestamp()
    xref_key = self.xrefKey + job_id
    return self.db.Get(xref_key)

  # lookup_job - abstract framework method
  def lookup_job(self, job_id):
    try:
      job_key = self.get_job_key(job_id)
      return self.get_job(job_key)
    except (KeyError, BaseException):
      return None

  # get_job_state - retrieve and unpickle the job item
  def get_job_state(self, job_key):
    # KeyError raised if key is not found
    job_state = self.db.Get(job_key)
    return pickle.loads(job_state)

  # get_job - retrieve job state and remake the job
  def get_job(self, job_key):
    # KeyError raised if key is not found
    job_state = self.db.Get(job_key)
    return self._reconstitute_job(job_state, 'JOB')

  # get_timestamp - return a timestamp, string or float format
  def get_timestamp(self, _datetime=None, floatMe=False):
    if not _datetime:
      _datetime = datetime.datetime.now()
    timestamp=datetime_to_utc_timestamp(_datetime)
    if floatMe:
      return timestamp
    return '%6f' % timestamp

  # _get_all_jobs - retrieve all jobs, returning an iterator
  def _get_all_jobs(self, dbKey, incVals=True, startTime=None, endTime=None):
    dbKey = self.__dict__[dbKey]
    startKey = dbKey + startTime if startTime else dbKey + '0'
    self._logger.debug('start key: ' + startKey)
    endKey = dbKey + endTime if endTime else dbKey + self.get_timestamp()
    self._logger.debug('end key: ' + endKey)
    return self.db.RangeIter(startKey, endKey,include_value=incVals) 

  # get_due_jobs - abstract framework method
  def get_due_jobs(self, now):
    timestamp = self.get_timestamp(now)
    cronIter = self._get_all_jobs('cronKey', endTime=timestamp)
    return self._reconstitute_jobs(cronIter, context='CRON')

  # get_next_run_time - abstract framework method
  def get_next_run_time(self):
    cronIter = self._get_all_jobs('cronKey')
    try:
      cron_key, cron_state = cronIter.next()
      self._logger.debug('next run time, cron key : ' + cron_key)
      cron_state = pickle.loads(cron_state)
      return utc_timestamp_to_datetime(cron_state['next_run_time'])
    except (StopIteration):
      return None
    except BaseException as ex:
      self._logger.error(str(ex))

  # get_all_jobs - abstract framework method
  def get_all_jobs(self):
    jobIter = self._get_all_jobs('jobKey')
    jobs = self._reconstitute_jobs(jobIter)
    self._fix_paused_jobs_sorting(jobs)
    return jobs

  # add_job - abstract framework method
  def add_job(self, job):
    try:
      #test if the job uuid xref already exists
      #this should never happen since uuid collision occurance is negligible
      job_key = self.get_job_key(job.id)
    except KeyError:
      pass
    else:
      raise ConflictingIdError(job.id)
    job_key = self.get_job_key()
    self._logger.debug('add job[%s] job_key : %s' % (job.id, job_key))
    try:
      #don't expect job_key timestamp conflict since the related scheduler call is synchronized   
      self.db.Get(job_key)
    except KeyError:
      pass
    else:
      #if conflict occurs the next attempt will be unique since now will be unique
      job_key = self.get_job_key()
    xref_key = self.xrefKey + job.id
    self.db.Put(xref_key, job_key, sync=True)
    if job.next_run_time:
      self.add_cron_item(job_key, job.next_run_time)
    job_state = job.__getstate__()
    self._logger.debug('job callable : ' + str(job_state['func']))
    self.db.Put(job_key, pickle.dumps(job_state,self.pickleProtocol), sync=True)

  # add_cron_item - add a job runtime item
  def add_cron_item(self, job_key, next_run_time):
    timestamp = self.get_timestamp(next_run_time, floatMe=True)
    cron_key = self.cronKey + '%6f' % timestamp
    cron_state = {'job_key':job_key, 'next_run_time':timestamp}
    self._logger.debug('add cron item[%s]: %s, %s' % (cron_key, job_key, str(next_run_time)))
    #don't need to cron job_key timestamp uniqueness since the related scheduler call is synchronized
    self.db.Put(cron_key, pickle.dumps(cron_state,self.pickleProtocol), sync=True)

  # update_job - abstract framework method
  def update_job(self, upd_job):
    try:
      job_key = self.get_job_key(upd_job.id)
    except KeyError:
      raise JobLookupError(upd_job.id)
    cur_job = self.get_job(job_key)
    if cur_job.next_run_time:
      self.remove_cron_item(cur_job.next_run_time)
    if upd_job.next_run_time:
      self.add_cron_item(job_key, upd_job.next_run_time)
    self.db.Put(job_key, pickle.dumps(upd_job.__getstate__(),self.pickleProtocol), sync=True)

  # remove_cron_item - remove a job runtime item
  def remove_cron_item(self, next_run_time):
    timestamp = self.get_timestamp(next_run_time)
    cron_key = self.cronKey + timestamp
    self.db.Delete(cron_key)

  # remove_job - abstract framework method
  def remove_job(self, job_id):
    try:
      job_key = self.get_job_key(job_id)
    except KeyError:
      raise JobLookupError(job_id)
    self._remove_job(job_key)   

  # _remove_job - remove job and cron items by job_state
  def _remove_job(self, job_key):
    try:
      job_state = self.get_job_state(job_key)
      if job_state['next_run_time']:
        self.remove_cron_item(job_state['next_run_time'])
      self.db.Delete(job_key)
      xref_key = self.xrefKey + job_state['id']
      self.db.Delete(xref_key)
    except KeyError:
      raise JobLookupError(job_key)
    except BaseException as ex:
      self._logger.exception('Failed to restore job state[%s], removing it ...' % job_key)
      self.db.Delete(job_key)

  # remove_all_jobs - abstract framework method
  def remove_all_jobs(self):
    jobIter = self._get_all_jobs('jobKey',incVals=False)
    while True:
      try:
        job_key = jobIter.next()
        self._remove_job(job_key)
      except StopIteration:
        break

  # _remove_cron_zombies - remove cron items, where the related job reference does not exist
  def remove_cron_zombies(self):
    cronIter = self._get_all_jobs('cronKey')
    while True:
      try:
        cron_key, cron_item = cronIter.next()
        self.remove_cron_zombie(cron_key, cron_item)
      except StopIteration:
        break

  # _remove_cron_zombie, remove if job_key does not exist
  def remove_cron_zombie(self, cron_key, cron_item):
    try:
      cron_item = pickle.loads(cron_item)
      job_key = cron_item['job_key']
      self.db.Get(job_key)
    except KeyError:
      self._logger.exception('Cron zombie found[%], removing it ..' % cron_key)
      self.db.Delete(cron_key)
    except BaseException as ex:
      self._logger.exception('Failed to restore cron item[%s], removing it ...' % cron_key)
      self.db.Delete(cron_key)

  # shutdown - abstract framework method
  def shutdown(self):
    # TO DO : add method to handle workbench shutdown if shutdown causes data loss
    pass

  # _reconstitute_job - remake and return job object
  def _reconstitute_job(self, item_state, context):
    item_state = pickle.loads(item_state)
    if context == 'CRON':
      job_key = item_state['job_key']
      item_state = self.get_job_state(job_key)
    job = Job.__new__(Job)
    job.__setstate__(item_state)
    job._scheduler = self._scheduler
    job._jobstore_alias = self._alias
    return job

  # _reconstitute_jobs - iterate over range, returning remade jobs
  def _reconstitute_jobs(self, itemIter, context='JOB'):
    jobs = []
    while True:
      try:
        item_key, item_state = itemIter.next()
        job = self._reconstitute_job(item_state, context)
        jobs.append(job)
      except StopIteration:
        break
      except BaseException as ex:
        self._logger.exception('Failed to restore job item[%s], removing it ..' % item_key)
        self.db.Delete(item_key)
    return jobs

  def __repr__(self):
    return '<%s>' % self.__class__.__name__

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
import os

apiBase = os.path.dirname(os.path.realpath(__file__))
activate_this = '%s/bin/activate_this.py' % apiBase
execfile(activate_this, dict(__file__=activate_this))

import logging
import sys
import json
import requests
from apitest import TestEmailPrvdr
from optparse import OptionParser
from threading import RLock

logger = logging.getLogger('apitest')
logFormat = '%(levelname)s:%(asctime)s %(message)s'
logFormatter = logging.Formatter(logFormat, datefmt='%d-%m-%Y %I:%M:%S %p')
logfile = '%s/log/testUtil.log' % apiBase
fileHandler = logging.FileHandler(logfile)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)

# -------------------------------------------------------------- #
# TestUtil
# ---------------------------------------------------------------#
class TestUtil(object):
  _lock = RLock()

	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def run(self):
    
    self.start()
    self._isRunning = self.isRunning()
    self._run()

	# -------------------------------------------------------------- #
	# start
	# ---------------------------------------------------------------#
  def start(self):

		try:
			pmetaFile = apiBase + '/apitest/wcPmeta.json'
			with open(pmetaFile,'r') as fhr:
				pmetadoc = fhr.read()
			pmetaAll = json.loads(pmetadoc)
			pmeta = pmetaAll['WcDirector']
			del(pmeta['globals'])
			pmeta.update(pmetaAll['Global'])
			
			tsXref = '180917173548'
			ciwork = pmeta['ciwork']
			session = '/WC' + tsXref
			ciwork += session
			proglib = ciwork + '/saslib'
			logdir = ciwork + '/saslib/log'
			
			pmeta['ciwork'] = ciwork
			pmeta['progLib'] = proglib			
			return pmeta
		except Exception as ex:
			logger.error('start failed : ' + str(ex))
			raise
    	
	# -------------------------------------------------------------- #
	# run
	# ---------------------------------------------------------------#
  def run(self):
    
		self.method = 'apiTestUtil.run'
		TestEmailPrvdr.init('UnitTest1809')
		pmeta = self.start()
		TestEmailPrvdr.start('UnitTest1809',pmeta)
		
		errmsg = 'batchTxnScheduleWC.sas has errored, refer log : '
		self.newMail('ERR1','sas script failure',errmsg)
		TestEmailPrvdr.sendMail('UnitTest1809')		

  # -------------------------------------------------------------- #
  # newMail
  # ---------------------------------------------------------------#
  def newMail(self, bodyKey, *args):
		TestEmailPrvdr.newMail('UnitTest1809',bodyKey,self.method,*args)	
  
if __name__ == '__main__':

	apiTest = TestUtil()
	apiTest.run()

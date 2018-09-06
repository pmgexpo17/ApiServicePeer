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
from apiservice import AppResolvar, logger
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from email.mime.text import MIMEText
from threading import RLock
import smtplib
import sys, yaml

# -------------------------------------------------------------- #
# EmailPacket
# ---------------------------------------------------------------#
class EmailPacket(object):
  
  def __init__(self):
    self.reset()

  # ------------------------------------------------------------ #
  # - setAttachment
  # -------------------------------------------------------------#
  def setAttachment(self, bindFile, fileName):
    
    with open(bindFile, "r") as fhr:
      mailPart = MIMEApplication(fhr.read(),Name=fileName)
    mailPart['Content-Disposition'] = 'attachment; filename=' + fileName
    self.attachment = mailPart

  # ------------------------------------------------------------ #
  # - wrap
  # -------------------------------------------------------------#
  def wrap(self, mailBody, bindFile=None,fileName=None):
    self.mailBody = MIMEText(mailBody)
    if bindFile:
      if not fileName:
        fileName = bindFile.split('/')[-1]
      self.setAttachment(bindFile,fileName)
    
  # ------------------------------------------------------------ #
  # - reset
  # -------------------------------------------------------------#
  def reset(self):
    self.attachment = None
    self.mailBody = ''
    self.scope = ''    
    self.subject = ''

# -------------------------------------------------------------- #
# CcEmailPrvdr
# ---------------------------------------------------------------#
class CcEmailPrvdr(AppResolvar):
  _lock = RLock()
  
  def __init__(self):
    self.mailbox = {}
    self.__dict__['ERR1'] = self.setBodyERR1
    self.__dict__['ERR2'] = self.setBodyERR2
    self.__dict__['ERR3'] = self.setBodyERR3
    self.__dict__['EOP1'] = self.setBodyEOP1
    
  # -------------------------------------------------------------- #
  # _start
  # ---------------------------------------------------------------#  
  @staticmethod
  def _start(mailKey, pmeta):

    mailer = CcEmailPrvdr.mailer = CcEmailPrvdr()
    mailer.jobTitle = pmeta['jobTitle']
    mailer.progLib = pmeta['progLib']
    mailer.workSpace = pmeta['workSpace']
    mailer._to = pmeta['userEmail'].split(' ')
    mailer._from = pmeta['userEmail']
    CcEmailPrvdr.subscribe(mailKey)
        
    assetFile = pmeta['assetLib'] + '/ccEmailTmplt.yaml'
    # put default error body for unmanaged exceptions
    mailer.meta = {'ERR1':['','']}
    mailer.meta['ERR1'][0] = 'CsvChecker has reported an error :<'
    mailer.meta['ERR1'][1] = '''
  Hi APP fasttracks team,

    CsvChecker has reported an error :<
    Error details :
      Source : %s
      Description : %s
      Message : %s

  Regards,
  APP cloud apps team
'''
    msgScope = 'emailPrvdr.CcEmailPrvdr._start'
    with open(assetFile,'r') as fhr:
      try:
        mailer.meta = yaml.load(fhr)
      except yaml.scanner.ScannerError as ex:
        CcEmailPrvdr.sendMail(mailKey,'ERR1',msgScope,'yaml parse error',str(ex))
        raise Exception(str(ex))
    try:
      mailer.meta['fromUser']
    except KeyError as ex:
      CcEmailPrvdr.sendMail(mailKey,'ERR1',msgScope,'yaml key error',str(ex))
      raise Exception(str(ex))
    else:
      mailer._from = mailer.meta['fromUser']

  # ------------------------------------------------------------ #
  # - _newMail
  # -------------------------------------------------------------#
  def _newMail(self, mailKey, bodyKey, *args):
    self.mailbox[mailKey] = self.wrapMail(bodyKey, *args)

  # ------------------------------------------------------------ #
  # - wrapMail
  # -------------------------------------------------------------#
  def wrapMail(self, bodyKey, *args):
    return self[bodyKey](*args)
    
  # ------------------------------------------------------------ #
  # - setBodyERR1
  # -------------------------------------------------------------#
  def setBodyERR1(self, msgScope, desc, message):

    logger.error('%s:%s,%s' % (msgScope,desc,message))
    packet = EmailPacket()
    packet.subject = self.meta['ERR1'][0] % self.jobTitle
    mailBody = self.meta['ERR1'][1]
    mailBody = mailBody % (msgScope,desc,message)
    packet.wrap(mailBody)
    return packet

  # ------------------------------------------------------------ #
  # - setBodyERR2
  # -------------------------------------------------------------#
  def setBodyERR2(self, msgScope, tableKey, scriptName):

    logFile = '%s/log/%s/%s.log' % (self.progLib, tableKey, scriptName)    
    logger.error('%s.sas has errored. refer %s' % (scriptName, logFile))
    packet = EmailPacket()
    packet.subject = self.meta['ERR2'][0] % self.jobTitle
    mailBody = self.meta['ERR2'][1]
    mailBody = mailBody % (scriptName,scriptName,self.workSpace)
    packet.wrap(mailBody,bindFile=logFile)
    return packet

  # ------------------------------------------------------------ #
  # - setBodyERR3
  # -------------------------------------------------------------#
  def setBodyERR3(self, msgScope, scriptName):

    logFile = '%s/log/%s.log' % (self.progLib, scriptName)    
    logger.error('%s.sas has errored. refer %s' % (scriptName, logFile))
    packet = EmailPacket()
    # the email template is exactly the same as ERR2
    packet.subject = self.meta['ERR2'][0] % self.jobTitle
    mailBody = self.meta['ERR2'][1]
    mailBody = mailBody % (scriptName,scriptName,self.workSpace)
    packet.wrap(mailBody,bindFile=logFile)
    return packet

  # ------------------------------------------------------------ #
  # - setBodyEOP1
  # -------------------------------------------------------------#
  def setBodyEOP1(self):
    
    packet = EmailPacket()
    packet.subject = self.meta['EOP1'][0] % self.jobTitle
    mailBody = self.meta['EOP1'][1]
    mailBody = mailBody % self.workSpace
    ccReport = self.progLib + '/evalCheckReport.lst'    
    packet.wrap(mailBody,bindFile=ccReport,fileName='csvChecker.lst')
    packet.wrap(mailBody)
    return packet

  # -------------------------------------------------------------- #
  # _sendMail
  # ---------------------------------------------------------------#
  def _sendMail(self, mailKey):
    packet = self.mailbox[mailKey]
    msg = MIMEMultipart()
    msg['Subject'] = packet.subject
    msg['From'] = self._from
    msg['To'] = COMMASPACE.join(self._to)
    msg['Date'] = formatdate(localtime=True)
    
    msg.attach(packet.mailBody)
    if packet.attachment:
      msg.attach(packet.attachment)

    smtp = smtplib.SMTP('smlsmtp')
    smtp.sendmail(self._from, self._to, msg.as_string())
    smtp.close()
    self.mailbox[mailKey] = None

  # -------------------------------------------------------------- #
  # hasMailReady
  # ---------------------------------------------------------------#  
  @staticmethod
  def hasMailReady(mailKey):
    with CcEmailPrvdr._lock:
      return CcEmailPrvdr.mailer.mailbox[mailKey] != None

  # ------------------------------------------------------------ #
  # - newMail
  # -------------------------------------------------------------#
  @staticmethod
  def newMail(mailKey, *args):
    with CcEmailPrvdr._lock:
      CcEmailPrvdr.mailer._newMail(mailKey, *args)

  # -------------------------------------------------------------- #
  # sendMail
  # ---------------------------------------------------------------#
  @staticmethod
  def sendMail(mailKey, *args):
    with CcEmailPrvdr._lock:
      mailer = CcEmailPrvdr.mailer
      if not mailer.mailbox[mailKey]:
        mailer._newMail(mailKey,*args)
      mailer._sendMail(mailKey)

  # -------------------------------------------------------------- #
  # subcribe
  # ---------------------------------------------------------------#  
  @staticmethod
  def subscribe(mailKey):
    with CcEmailPrvdr._lock:      
      CcEmailPrvdr.mailer.mailbox[mailKey] = None
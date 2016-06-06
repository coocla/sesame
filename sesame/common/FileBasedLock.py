#coding:utf-8
import time
import sys
import os
import fcntl
import commands

from sesame.common.logger import LoggerFactory
logger = LoggerFactory.getLogger(__name__)

class FileBasedLock(object):
    '''
    classdocs
    Note: this lock only apply to process level, does not work for thread level,
    for thread level lock, please use thread.Lock()
    '''


    def __init__(self, lockFilePath=None):
        '''
        Constructor
        '''
        self.lockFilePath = lockFilePath
    
    def acquire(self, acquireTimeout=60):
        #acquireTimeout Unit is sec
        acquired = False
        try:
            logger.debug("Try to get lock %s" % self.lockFilePath)
            self.createLocFileIfNotExist()
            self.lockFile= open(self.lockFilePath, 'w')
            fcntl.lockf(self.lockFile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            logger.debug("Get lock %s successfully!" % self.lockFilePath)
            acquired = True
        except:
            logger.error("Fail to get lock, error=%s!" % sys.exc_info()[0])
            acquired = False
        
        if acquired :
            iLockTime=time.time()
            lock_time=str(iLockTime)
            self.lockFile.write(lock_time)
            return True
        else :
            acquireTime = time.time()
            while time.time() - acquireTime < acquireTimeout :
                pollingInterval=5 
                time.sleep(pollingInterval)
                try:
                    logger.debug("Try to get lock %s ..." % self.lockFilePath)
                    fcntl.lockf(self.lockFile, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    logger.debug("Get lock %s successfully!" % self.lockFilePath)
                    acquired = True
                    break
                except :
                    logger.debug("Fail to get lock %s, error=%s!" % (self.lockFilePath, sys.exc_info()[0]))
                acquired = False
        return acquired
    
    def createLocFileIfNotExist(self):
        try:
            dir_path = os.path.dirname(self.lockFilePath)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            
            if not os.path.exists(self.lockFilePath):
                cmd = "touch {lock_file_path}".format(lock_file_path=self.lockFilePath)
                commands.getoutput(cmd)
        except Exception, e:
            logger.error(e, exc_info=True)
    
    def release(self):
        try:
            logger.debug("Release lock...")
            fcntl.flock(self.lockFile, fcntl.LOCK_UN) 
            logger.debug("Release lock successfully!")
        except :
            logger.debug("Release lock %s exception=%s" % (self.lockFilePath, sys.exc_info()[0]))
        finally :
            try:
                if not self.lockFile.closed :
                    self.lockFile.close()
            except :
                logger.debug("lockFile.close %s exception=%s" % (self.lockFilePath, sys.exc_info()[0]))

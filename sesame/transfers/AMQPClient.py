#coding:utf-8
import commands
import os
import sys
import datetime

from sesame.common.FileUtil import FileUtil

from sesame.common.logger import LoggerFactory
logger = LoggerFactory.getLogger(__name__)

class AMQPClient():
    chan = None
    conn = None
    
    def __init__(self, 
                 receivedQueueLock=None,
                 receivedQueuePath=None,
                 sendQueueLock=None,
                 sendQueuePath=None,
                 ):
        '''
        Constructor
        '''
        self.receivedQueueLock = receivedQueueLock
        self.receivedQueuePath = receivedQueuePath
        self.sendQueueLock = sendQueueLock
        self.sendQueuePath = sendQueuePath
    
    def receive(self):
        messages = []
        if os.path.exists(self.receivedQueuePath) :
            #get all the messages and sent out
            fileNames = os.listdir(self.receivedQueuePath)
            
            if fileNames!=None and len(fileNames) > 0 :
                try :
                    for fileName in fileNames :
                        messagePath = "%s/%s" % (self.receivedQueuePath, fileName)
                        if os.path.isfile(messagePath) :
                            message = FileUtil.readContent(messagePath)
                            messages.append(message)
                            os.remove(messagePath)
                            logger.debug("msg %s is fetched from queue successfully!" % messagePath)
                    pass
                except Exception, e:
                    logger.error(e)
                    logger.error("MessageQueue Unexpected error: %s", sys.exc_info()[0])
        return messages
    
    def send(self, event, message, messageTimestamp=None):
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        msgPath = "%s/%s-%s-%s-s%s-%s" % (self.sendQueuePath, timestamp,
                event.EventUUID, event.EventUser, event.EventID, event.StepID)
        try:
            FileUtil.writeContent(msgPath, message)
        except Exception, e :
            logger.error(e)

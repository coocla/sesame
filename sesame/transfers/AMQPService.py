#coding:utf-8
import os
import sys
import time
import traceback
import threading
from datetime import datetime, timedelta

from sesame.transfers.AMQPReceiver import AMQPReceiver
from sesame.transfers.AMQPSender import AMQPSender
from sesame.transfers.ConnectionStatus import ConnectionStatus

from sesame.common.logger import LoggerFactory
logger = LoggerFactory.getLogger(__name__)

class AMQPService(object):
    '''
    启动本地的 AMQP 消息接收和结果发送线程
    '''
    def __init__(self, 
                 agentData = None,
                 amqpReceiver=None,
                 amqpSender=None,
                 receivedQueueLock=None,
                 receivedQueuePath=None,
                 sendQueueLock=None,
                 sendQueuePath=None,
                 sendQueueBufferPath=None
                 ):
        self.agentData = agentData
        self.receivedQueueLock = receivedQueueLock
        self.receivedQueuePath = receivedQueuePath
        self.sendQueueLock = sendQueueLock
        self.sendQueuePath = sendQueuePath
        self.sendQueueBufferPath = sendQueueBufferPath
    
    def start(self):

        def amqpReceiverSenderHAService():
            amqpReceiver = AMQPReceiver(self.agentData, self.receivedQueueLock, self.receivedQueuePath)
            receiverThread = AMQPReceiverThread(amqpReceiver)
            receiverThread.start()
            logger.info("Start amqpReceiver Service")
            
            amqpSender = AMQPSender(self.agentData, 
                                    self.sendQueueLock, 
                                    self.sendQueuePath,
                                    self.sendQueueBufferPath)
            senderThread = AMQPSenderThread(amqpSender)
            senderThread.start()
            logger.info("Start amqpSender Service")
        
            while True:
                if amqpReceiver.getConnectionStatus() == ConnectionStatus.DISCONNECTED:
                    logger.debug("amqpReceiver.getConnectionStatus()=%s" % amqpReceiver.getConnectionStatus())
                if amqpSender.getConnectionStatus() == ConnectionStatus.DISCONNECTED:
                    logger.debug("amqpSender.getConnectionStatus()=%s" % amqpSender.getConnectionStatus())
                try:
                    receiverDisconnectedTime = amqpReceiver.getDisconnectedTime()
                    if receiverDisconnectedTime!=None :
                        logger.debug("amqpReceiver.receiverDisconnectedTime=%s" % receiverDisconnectedTime.strftime('%Y%m%d%H%M%S'))
                        if datetime.now() - timedelta(minutes=1) > receiverDisconnectedTime :
                            try:
                                amqpReceiver.stop()
                            except :
                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                                logger.error(''.join('!! ' + line for line in lines))
                                logger.error("AMQPService stop amqpReceiver Unexpected error: %s", sys.exc_info()[0])
                            amqpReceiver = AMQPReceiver(self.agentData, self.receivedQueueLock, self.receivedQueuePath)
                            receiverThread = AMQPReceiverThread(amqpReceiver)
                            receiverThread.start()
                    else :
                        logger.debug("amqpReceiver.receiverDisconnectedTime=None")
                    
                    logger.debug("Go to check if amqpSender disconnected")
                    
                    senderDisconnectedTime = amqpSender.getDisconnectedTime()
                    if senderDisconnectedTime!=None :
                        logger.debug("amqpSender.senderDisconnectedTime=%s" % senderDisconnectedTime.strftime('%Y%m%d%H%M%S'))
                        if datetime.now() - timedelta(minutes=1) > senderDisconnectedTime :
                            try:
                                amqpSender.stop()
                            except :
                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                                logger.error(''.join('!! ' + line for line in lines))
                                logger.error("AMQPService stop amqpSender Unexpected error: %s", sys.exc_info()[0])
                            amqpSender = AMQPSender(self.agentData, 
                                                    self.sendQueueLock, 
                                                    self.sendQueuePath,
                                                    self.sendQueueBufferPath)
                            senderThread = AMQPSenderThread(amqpSender)
                            senderThread.start()
                    else :
                        logger.debug("amqpSender.receiverDisconnectedTime=None")
                    
                    logger.debug("Go to check if amqpSender works normally, sent msg and get ack in time!")
                    #check tosendMsg queue, if there are msg that not sent in 1 min, stop AMQPSender and start new AMQPSender

                    senderConnectionStatus = amqpSender.getConnectionStatus()
                    
                    sentNotAckPendingTooLong = self.isThereMsgPendingTooLong(self.sendQueueBufferPath)
                    tosentPendingTooLong = self.isThereMsgPendingTooLong(self.sendQueuePath)
                    
                    logger.debug("sentNotAckPendingTooLong=%s, tosentPendingTooLong=%s" % (sentNotAckPendingTooLong, tosentPendingTooLong))
                    if sentNotAckPendingTooLong or tosentPendingTooLong :
                        if senderConnectionStatus == ConnectionStatus.CONNECTED :
                            logger.debug("Go to sleep 150 secs to waiting for amqpSender and amqpReceiver to restart!")
                            time.sleep(150)
                            logger.debug("After sleep 150 secs to waiting for amqpSender and amqpReceiver to restart!")
                            try:
                                amqpSender.stop()
                            except Exception,e:
                                logger.error(e, exc_info=True)
                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                                logger.error(''.join('!! ' + line for line in lines))
                                logger.error("AMQPService stop amqpSender Unexpected error: %s", sys.exc_info()[0])
                            
                            amqpSender = AMQPSender(self.agentData, 
                                                    self.sendQueueLock, 
                                                    self.sendQueuePath,
                                                    self.sendQueueBufferPath)
                            senderThread = AMQPSenderThread(amqpSender)
                            senderThread.start()
                            
                            try:
                                amqpReceiver.stop()
                            except Exception,e:
                                logger.error(e, exc_info=True)
                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                                logger.error(''.join('!! ' + line for line in lines))
                                logger.error("AMQPService stop amqpReceiver Unexpected error: %s", sys.exc_info()[0])
                            
                            amqpReceiver = AMQPReceiver(self.agentData, self.receivedQueueLock, self.receivedQueuePath)
                            receiverThread = AMQPReceiverThread(amqpReceiver)
                            receiverThread.start()
                            
                        pass
                    
                except Exception,e:
                    logger.error(e, exc_info=True)
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    logger.error(''.join('!! ' + line for line in lines))

                logger.debug("Go to sleep 30 secs to check amqpSender and amqpReceiver")
                time.sleep(30)
                logger.debug("Wake up from sleep 30 secs to check amqpSender and amqpReceiver")
        
        t = threading.Thread(target=amqpReceiverSenderHAService)
        t.start()
    
    def isThereMsgPendingTooLong(self, msgDir):
        pendingTooLong = False

        if self.sendQueueLock.acquire(60) :
            fileNames = os.listdir(msgDir)
            for fileName in fileNames :
                fields = fileName.split("-")
                strMsgTime = fields[0]
                msgTime = datetime.strptime(strMsgTime, "%Y%m%d%H%M%S")
                if datetime.now() - timedelta(minutes=2) > msgTime :
                    pendingTooLong = True
                    logger.debug("Message %s/%s pending more than 2 mins to sent or ack" % (msgDir, fileName))
                    break
                pass
            self.sendQueueLock.release()
        return pendingTooLong
        
    
class AMQPReceiverThread(threading.Thread):
    
    def __init__(self, amqpReceiver):
        threading.Thread.__init__(self)
        self.amqpReceiver = amqpReceiver
        
    def run(self):
        self.amqpReceiver.run()

class AMQPSenderThread(threading.Thread):
    
    def __init__(self, amqpSender):
        threading.Thread.__init__(self)
        self.amqpSender = amqpSender
        
    def run(self):
        self.amqpSender.run()            
            

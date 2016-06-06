#coding:utf-8
import os
import sys
import time

from sesame import AppContext as cfg
from sesame.model.agentData import agentData
from sesame.model.Event import EventModel
from sesame.common.FileUtil import FileUtil
from sesame.common.FileBasedLock import FileBasedLock
from sesame.application.EventService import EventService
from sesame.transfers.AMQPService import AMQPService
from sesame.transfers.AMQPSender import AMQPSender
from sesame.transfers.AMQPReceiver import AMQPReceiver
from sesame.transfers.AMQPClient import AMQPClient
from sesame.common.logger import LoggerFactory
logger = LoggerFactory.getLogger(__name__)

class AgentApp(object):
    def __init__(self, agent_json):
        self.agent_json = agent_json

    def start(self):
        logger.info("Start AMQPService Service!")
        if not os.path.exists(self.agent_json):
            logger.error("%s no such file or directory" % self.agent_json)
            sys.exit(6)
        AgentData = agentData.fromJSON(FileUtil.readContent(self.agent_json))

        receivedQueueLock = FileBasedLock(cfg.receivedQueueLockFile)
        sendQueueLock = FileBasedLock(cfg.sendQueueLockFile)
        amqpService = AMQPService(AgentData,
                amqpReceiver=AMQPReceiver(AgentData,
                    receivedQueueLock=receivedQueueLock,
                    receivedQueuePath=cfg.receivedQueuePath),
                amqpSender=AMQPSender(AgentData,
                    sendQueueLock=sendQueueLock,
                    sendQueuePath=cfg.sendQueuePath,
                    sendQueueBufferPath=cfg.sendQueueBufferPath),
                receivedQueueLock=receivedQueueLock,
                receivedQueuePath=cfg.receivedQueuePath,
                sendQueueLock=sendQueueLock,
                sendQueuePath=cfg.sendQueuePath,
                sendQueueBufferPath=cfg.sendQueueBufferPath)
        amqpService.start()
        eventService = EventService(
                eventModel=EventModel(AgentData),
                agent_id=AgentData.agent_id,
                queueClient=AMQPClient(
                    receivedQueueLock=receivedQueueLock,
                    receivedQueuePath=cfg.receivedQueuePath,
                    sendQueueLock=sendQueueLock,
                    sendQueuePath=cfg.sendQueuePath),
                receivedQueueLock=receivedQueueLock,
                receivedQueuePath=cfg.receivedQueuePath)
        eventService.start()

if __name__ == '__main__':
    agent = AgentApp('/data/sesame/etc/sesame.json')
    agent.start()

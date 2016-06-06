#coding:utf-8
import time
import json
import base64
import traceback
import datetime
import threading
import socket

from sesame.model.Event import Event
from sesame.application.handlers.ExecuteScriptHandler import ExecuteScriptHandler
from sesame.common.logger import LoggerFactory
logger = LoggerFactory.getLogger(__name__)

class EventService(object):
    """
    消息的worker, 处理对应的event
    """
    def __init__(self, eventModel=None,
            agent_id=None,
            queueClient=None,
            receivedQueueLock=None,
            receivedQueuePath=None):
        self.eventModel = eventModel
        self.agent_id = agent_id
        self.queueClient = queueClient
        self.receivedQueueLock = receivedQueueLock
        self.receivedQueuePath = receivedQueuePath

    def start(self):
        def EventListener():
            while True:
                messages = self.queueClient.receive()
                if messages!=None and len(messages)>0:
                    for message in messages:
                        EventThread = HandleEventThread(message, self)
                        EventThread.start()
                time.sleep(5)

        t = threading.Thread(target=EventListener)
        t.start()

    def go(self, event):
        event_result = {"rc": -1, "stdout": "NULL", "stderr": "NULL", "agent_id": self.agent_id}
        logger.info("Event Name is %s" % event.EventName)
        try:
            event_type = event.EventType
            if not hasattr(self, event_type):
                event_type = "default"
            event_handler = getattr(self, event_type)(self.eventModel)
            event_result = event_handler.handler(self.agent_id, event)
        except Exception,e:
            traceback.print_exc()
            logger.error(e)
        finally:
            self.sendEventResponse(event, event_result)

    def default(self, eventModel):
        '''event_type: ExecScript'''
        return ExecuteScriptHandler(eventModel=eventModel)

    def sendEventResponse(self, event, event_result):
        ResponseEventName = "%s.Response" % event.EventID
        respevent = event.toDict()
        respevent["event_usetime"] = "%.2f" % (event.FinishAt-event.StartAt)
        respevent["event_name"] = ResponseEventName
        respevent["event_method"] = "RESPONSE"
        respevent["event_data"] = base64.encodestring(json.dumps(event_result)).strip()
        responseEvent = self.eventModel.createEvent(**respevent)

        logger.info("Send event %s response=%s" % (event.EventID, responseEvent.toJSON()))
        if event.EventReturn == "socket":
            try:
                s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect(event.EventSocket)
                s.send(responseEvent.toJSON())
                s.close()
            except:
                pass
        else:
            self.queueClient.send(event, responseEvent.toJSON())


class HandleEventThread(threading.Thread):
    def __init__(self, message, eventService):
        super(HandleEventThread, self).__init__()
        self.message = message
        self.eventService = eventService

    def run(self):
        try:
            event = Event.fromJSON(self.message)
            if event.EventMethod == "REQUEST":
                self.eventService.go(event)
            else:
                logger.warn("Reciver invalid event=%s" % event.toJSON())
        except Exception,e:
            logger.exception(e)


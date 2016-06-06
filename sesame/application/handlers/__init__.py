#coding:utf-8

class BaseHander(object):
    def __init__(self, eventModel):
        self.eventModel = eventModel

    def handler(self, event, timeout=None):
        pass

    def EventReponse(self, event, event_result):
        pass

#coding:utf-8
import time
import json

class EventModel(object):
    def __init__(self, agentData=None):
        self.agentData = agentData

    def createEvent(self, **kwargs):
        event = Event(**kwargs)
        event.EventTimeStamp = int(time.time()*1000)
        return event


class Event(object):
    event_id = ""           #Flow id
    event_uuid = ""         #Task uuid
    event_type = ""         #ExecuteScript
    event_method = ""       #REQUEST/RESPONSE
    event_data = ""         #EventMetadata
    event_source = ""       #EventSource
    event_timestamp = ""    #Timestamp
    event_name = ""         #
    event_user = ""         #EventUser
    event_channel = ""      #EventChannel
    event_environ = ""      #EventEnviron
    event_timeout = None    #
    event_args = ""         #
    event_return = ""       #返回方式
    socket = ""             #返回的地址
    remote_ip = ""          #
    step_id = ""            #
    start_at = ""           #
    event_usetime = ""      #
    finish_at = ""          #
    flow_uuid = ""          
    rabbitmq_vhost = ""     #
    rabbitmq_exchange = ""  #

    def __init__(self, event_id=None, event_uuid=None, socket=(),
            event_type=None, event_method=None, event_data=None,
            event_source=None, event_timestamp=None, event_name=None,
            event_user=None, event_channel=None, rabbitmq_vhost=None,
            rabbitmq_exchange=None, event_environ=None, step_id=None,
            event_timeout=event_timeout, event_args=None, flow_uuid=None,
            start_at=None, finish_at=None, remote_ip=None, event_usetime=None,
            event_return="amqp"):
        self.EventID = event_id
        self.EventUUID = event_uuid
        self.EventType = event_type
        self.EventMethod = event_method
        self.EventData = event_data
        self.EventSource = event_source
        self.EventTimeStamp = event_timestamp
        self.EventName = event_name
        self.EventUser = event_user
        self.EventChannel = event_channel
        self.rabbitmq_vhost = rabbitmq_vhost
        self.rabbitmq_exchange = rabbitmq_exchange
        self.EventEnviron = event_environ
        self.EventTimeout = event_timeout
        self.EventArgs = event_args
        self.EventReturn = event_return
        self.EventSocket = socket
        self.StepID = step_id
        self.FlowUUID = flow_uuid
        self.StartAt = start_at
        self.FinishAt = finish_at
        self.RemoteIP = remote_ip
        self.EventUseTime = event_usetime

    def toDict(self):
        event_dict = {}
        event_dict["event_id"] = self.EventID
        event_dict["event_uuid"] = self.EventUUID
        event_dict["event_type"] = self.EventType
        event_dict["event_method"] = self.EventMethod
        event_dict["event_data"] = self.EventData
        event_dict["event_source"] = self.EventSource
        event_dict["event_timestamp"] = self.EventTimeStamp
        event_dict["event_name"] = self.EventName
        event_dict["event_user"] = self.EventUser
        event_dict["event_channel"] = self.EventChannel
        event_dict["rabbitmq_vhost"] = self.rabbitmq_vhost
        event_dict["rabbitmq_exchange"] = self.rabbitmq_exchange
        event_dict["event_environ"] = self.EventEnviron
        event_dict["event_timeout"] = self.EventTimeout
        event_dict["event_args"] = self.EventArgs
        event_dict["step_id"] = self.StepID
        event_dict["flow_uuid"] = self.FlowUUID
        event_dict["start_at"] = self.StartAt
        event_dict["finish_at"] = self.FinishAt
        event_dict["remote_ip"] = self.RemoteIP
        event_dict["event_usetime"] = self.EventUseTime
        event_dict["event_return"] = self.EventReturn
        event_dict["socket"] = self.EventSocket
        return event_dict

    def setEventData(self, event_data):
        self.EventData = event_data

    def setEventUser(self, event_user):
        self.EventUser = event_user

    def setEventMethod(self, event_method):
        self.EventMethod = event_method

    def setEventEnv(self, event_env):
        self.EventEnviron = event_env

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)

    @staticmethod
    def fromJSON(event_json):
        event_dict = json.loads(event_json)
        event_id = event_dict["event_id"]
        event_uuid = event_dict["event_uuid"]
        event_type = event_dict["event_type"]
        event_method = event_dict["event_method"]
        event_data = event_dict["event_data"]
        event_source = event_dict["event_source"]
        event_timestamp = event_dict["event_timestamp"]
        event_name = event_dict["event_name"]
        event_user = event_dict["event_user"]
        event_channel = event_dict["event_channel"]
        rabbitmq_vhost = event_dict["rabbitmq_vhost"]
        rabbitmq_exchange = event_dict["rabbitmq_exchange"]
        event_environ = event_dict["event_environ"]
        event_timeout = event_dict.get("event_timeout", None)
        event_args = event_dict.get("event_args", "")
        step_id = event_dict.get("step_id", 1)
        flow_uuid = event_dict["flow_uuid"]
        start_at = event_dict["start_at"]
        finish_at = event_dict["finish_at"]
        remote_ip = event_dict["remote_ip"]
        event_usetime = event_dict["event_usetime"]
        event_return = event_dict.get("event_return", "amqp")
        socket = event_dict.get("socket", ())
        return Event(event_id=event_id, event_uuid=event_uuid, event_type=event_type,
                event_method=event_method, event_data=event_data, event_source=event_source,
                event_timestamp=event_timestamp, event_name=event_name, event_user=event_user,
                event_channel=event_channel, rabbitmq_vhost=rabbitmq_vhost, event_return=event_return,
                rabbitmq_exchange=rabbitmq_exchange, event_environ=event_environ, step_id=step_id,
                event_timeout=event_timeout, event_args=event_args, flow_uuid=flow_uuid, socket=socket,
                start_at=start_at, finish_at=finish_at, remote_ip=remote_ip, event_usetime=event_usetime)

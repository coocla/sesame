#coding:utf-8
import json
import base64

class agentData(object):
    """
    将文件中的JSON配置转化为object
    """
    def __init__(self, rabbitmq_host=None, rabbitmq_port=None, rabbitmq_ssl=None,
            rabbitmq_down_exchange=None, rabbitmq_down_queue=None, rabbitmq_up_queue=None,
            rabbitmq_up_exchange=None, rabbitmq_vhost=None, queue_password=None, 
            queue_username=None, agent_id=None):
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.rabbitmq_ssl = rabbitmq_ssl
        self.rabbitmq_down_exchange = rabbitmq_down_exchange
        self.rabbitmq_down_queue = rabbitmq_down_queue
        self.rabbitmq_up_exchange = rabbitmq_up_exchange
        self.rabbitmq_up_queue = rabbitmq_up_queue
        self.rabbitmq_vhost = rabbitmq_vhost
        self.queue_password = queue_password
        self.queue_username = queue_username
        self.agent_id = agent_id

    def toDict(self):
        agent_data_dict = {}
        agent_data_dict["rabbitmq_host"] = self.rabbitmq_host
        agent_data_dict["rabbitmq_port"] = self.rabbitmq_port
        agent_data_dict["rabbitmq_ssl"] = self.rabbitmq_ssl
        agent_data_dict["rabbitmq_down_exchange"] = self.rabbitmq_down_exchange
        agent_data_dict["rabbitmq_down_queue"] = self.rabbitmq_down_queue
        agent_data_dict["rabbitmq_up_exchange"] = self.rabbitmq_up_exchange
        agent_data_dict["rabbitmq_up_queue"] = self.rabbitmq_up_queue
        agent_data_dict["rabbitmq_vhost"] = self.rabbitmq_vhost
        agent_data_dict["queue_password"] = self.queue_password
        secret_key = self.queue_username
        agent_data_dict["queue_username"] = base64.encodestring(\
                secret_key).replace("-", "equal").strip()
        agent_data_dict["agent_id"] = self.agent_id
        return agent_data_dict

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)

    @staticmethod
    def fromJSON(agent_data_json):
        agent_data_dict = json.loads(agent_data_json)

        rabbitmq_host = agent_data_dict["rabbitmq_host"]
        rabbitmq_port = agent_data_dict["rabbitmq_port"]
        rabbitmq_ssl = agent_data_dict["rabbitmq_ssl"]
        rabbitmq_down_exchange = agent_data_dict["rabbitmq_down_exchange"]
        rabbitmq_down_queue = agent_data_dict["rabbitmq_down_queue"]
        rabbitmq_up_exchange = agent_data_dict["rabbitmq_up_exchange"]
        rabbitmq_up_queue = agent_data_dict["rabbitmq_up_queue"]
        rabbitmq_vhost = agent_data_dict["rabbitmq_vhost"]

        queue_username = agent_data_dict["queue_username"]
        queue_password = agent_data_dict["queue_password"]

        queue_username = base64.decodestring(queue_username.replace("equal", "="))
        agent_id = agent_data_dict["agent_id"]

        return agentData(rabbitmq_host=rabbitmq_host,
                rabbitmq_port=rabbitmq_port, rabbitmq_ssl=rabbitmq_ssl,
                rabbitmq_down_exchange=rabbitmq_down_exchange,
                rabbitmq_down_queue=rabbitmq_down_queue,
                rabbitmq_up_exchange=rabbitmq_up_exchange,
                rabbitmq_up_queue=rabbitmq_up_queue,
                rabbitmq_vhost=rabbitmq_vhost,
                queue_password=queue_password,
                queue_username=queue_username,
                agent_id=agent_id)

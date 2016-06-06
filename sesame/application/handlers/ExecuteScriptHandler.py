#coding:utf-8
import os
import stat
import time
import datetime
import commands

from sesame import AppContext as cfg
from sesame.application.handlers import BaseHander
from sesame.common import subprocess
from sesame.common.FileUtil import FileUtil
from sesame.common.logger import LoggerFactory
logger = LoggerFactory.getLogger(__name__)


class ExecuteScriptHandler(BaseHander):
    def __init__(self, eventModel):
        super(ExecuteScriptHandler, self).__init__(eventModel)
        self.eventModel = eventModel

    def handler(self, agent_id, event):
        #logger.info("ExecuteScriptHandler.handler.event=%s" % event.toJSON())
        event_data = event.EventData
        logger.debug("script_content=%s" % event_data)

        result = {"rc": 0, "stdout": "NULL", "stderr": "NULL", "agent_id": agent_id}
        now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        try:
            userHandlerPath = self.configHandlerEvn(event, now)
            shell_env = os.environ.copy()
            if 'PYTHONIOENCODING' not in shell_env:
                shell_env["PYTHONIOENCODING"] = "utf-8"
            if event.EventEnviron:
                user_env = dict((k,str(v)) for k,v in event.EventEnviron.iteritems())
                shell_env.update(user_env)
            else:
                user_env = {}
            shell_env.update({"agent_id": str(agent_id)})
            logger.info("cmd=%s" % userHandlerPath)
            logger.info("Environ=%s" % shell_env["sid"])

            event.StartAt = time.time()
            proc = subprocess.Popen(userHandlerPath, shell=True, close_fds=True, stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, env=shell_env)
            try:
                output, error = proc.communicate(timeout=event.EventTimeout)
            except subprocess.TimeoutExpired:
                output, error = ("NULL", "TimeOut...")
                result["rc"] = -9999
            else:
                result["rc"] = proc.returncode

            event.FinishAt = time.time()
            result["stdout"] = output.strip()
            result["stderr"] = error

        except Exception,e:
            logger.error(e, exc_info=True)
        finally:
            userHandlerPath = self.getHandlerPath(event, now)
            commands.getoutput("rm -rf %s" % userHandlerPath)
        return result

    def getHandlerPath(self, event, now):
        return "%s/executeScript.s%s-%s.%s" % (cfg.ExecuteTmpPath, event.EventID, event.StepID, now)

    def configHandlerEvn(self, event, now):
        handerContent = event.EventData
        userHandlerPath = self.getHandlerPath(event, now)
        FileUtil.writeContent(userHandlerPath, handerContent)
        FileUtil.dos2unix(userHandlerPath)
        # 添加执行权限
        os.chmod(userHandlerPath, stat.S_IRWXU|stat.S_IRGRP|stat.S_IROTH) #755
        return userHandlerPath + " " + event.EventArgs

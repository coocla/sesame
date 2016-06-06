#coding:utf-8
import os
import logging

from sesame import AppContext as cfg

class LoggerFactory(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
    
    @staticmethod
    def getLogger(logger_name):
        logger = logging.getLogger(logger_name)

        logger.setLevel(getattr(logging, cfg.LogLevel.upper(), "DEBUG"))
        formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s [%(name)s %(lineno)d] %(message)s", \
                datefmt="%Y-%m-%d %H:%M:%S")

        if not os.path.exists(os.path.dirname(cfg.LogFile)):
            os.makedirs(os.path.dirname(cfg.LogFile))
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        filehandler = logging.FileHandler(cfg.LogFile)
        filehandler.setFormatter(formatter)

        logger.addHandler(console)
        logger.addHandler(filehandler)

        return logger

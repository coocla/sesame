'''
Created on Feb 14, 2015

@author: jason@fit2cloud.com
'''

import threading

class ThreadingLock(object):
    '''
    classdocs
    '''


    def __init__(self, name=None):
        '''
        Constructor
        '''
        self.name = name
        self.lock = threading.Lock()
        pass
    
    def accquire(self):
        self.lock.acquire()
        
    def release(self):
        self.lock.release()
    
    def getName(self):
        return self.name
        
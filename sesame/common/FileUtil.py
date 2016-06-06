#coding:utf-8
import os
import hashlib
import random
import sys

from sesame.common.logger import LoggerFactory
logger = LoggerFactory.getLogger(__name__)

class FileUtil(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
    @staticmethod
    def readContent(file_path):
        config_file = file(file_path, 'r')
        file_content = ""
        file_lines = config_file.readlines();
        for line in file_lines :
            file_content = file_content + line
        config_file.close()
        return file_content
    
    @staticmethod
    def writeContent(file_path, content):
        dir_path = os.path.dirname(file_path)
        os.system("mkdir -p %s" % dir_path)
        try:
            config_file = file(file_path, 'w')
            config_file.write(content)
            config_file.close()
        except Exception, e:
            logger.error(e)
    
    @staticmethod
    def dos2unix(file_path):
        config_file = file(file_path, 'rU')
        file_content = ""
        file_lines = config_file.readlines();
        for line in file_lines :
            file_content = file_content + line
        config_file.close()
        
        FileUtil.writeContent(file_path, file_content)
    
    #get md5 of a input string
    @staticmethod
    def getStringMD5(str):
        strMd5 = hashlib.md5(str).hexdigest()
        return strMd5
    
    
    #get md5 of a input file
    @staticmethod
    def getFileMD5(filePath):
        fileinfo = os.stat(filePath)
        if int(fileinfo.st_size)/(1024*1024)>1000:
            return FileUtil.getBigFileMD5(filePath)
        
        fileContent = FileUtil.readContent(filePath)
        strMd5 = hashlib.md5(fileContent).hexdigest()
        return strMd5
    
    
    #get md5 of a input bigfile
    @staticmethod
    def getBigFileMD5(filePath):
        m = hashlib.md5()
        f = open(filePath,'rb')
        maxbuf = 8192
    
    
        while 1:
            buf = f.read(maxbuf)
            if not buf:
                break
            m.update(buf)
    
    
        f.close()
        return m.hexdigest()
    
    
    #get md5 of a input folder.
    #result will be output to the specified file
    @staticmethod
    def getBetchFilesMD5(dir,outMD5File):
        outfile = open(outMD5File,'w')
        for root ,subdirs, files in os.walk(dir):
            for file in files:
                filefullpath = os.path.join(root,file)
                md5 = FileUtil.getFileMD5(filefullpath)
                outfile.write(file+'   md5:   '+md5+"\n")
        outfile.close()

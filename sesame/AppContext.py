#coding:utf-8
######### Lock Config ##########
receivedQueueLockFile="/var/sesame/locks/receivedQueueLock"
sendQueueLockFile="/var/sesame/locks/sendQueueLock"
amqpConnectionLockFile="/var/sesame/locks/amqpConnectionLock"

######### Queue Config ##########
receivedQueuePath="/var/sesame/data/receivedQueue"
sendQueuePath="/var/sesame/data/sendQueue"
sendQueueBufferPath="/var/sesame/data/sendBuffer"

######### Global Config ##########
ExecuteTmpPath="/tmp/sesame"
LogLevel = "debug"
LogFile = "/var/sesame/sesame.log"

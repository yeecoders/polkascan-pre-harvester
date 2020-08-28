import sys
import os
from app.tasks import dealWithForks
import time
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
#def dealWithForks(self, shard_num, bid=None, substrate_url=None):

result = dealWithForks.delay(0, 1000, 'http://10.32.18.239:9930/')
while not result.ready():
    time.sleep(1)
print('task done: {0}'.format(result.get()))

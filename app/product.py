import sys
import os
from app.tasks import dealWithForks
import time
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

result = dealWithForks.delay()
while not result.ready():
    time.sleep(1)
print('task done: {0}'.format(result.get()))

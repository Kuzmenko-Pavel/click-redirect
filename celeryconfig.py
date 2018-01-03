# coding: utf-8
import sys
sys.path.append('.')


BROKER_URL = 'pyamqp://click-redirect:123qwe@srv-4.yottos.com:5672/click-redirect'
BROKER_CONNECTION_MAX_RETRIES = 0
BROKER_HEARTBEAT = 0
CELERY_TASK_IGNORE_RESULT = True
CELERY_IMPORTS = ("tasks",)
CELERY_TASK_RESULT_EXPIRES = 1
CELERY_RESULT_PERSISTENT = False
CELERY_ENABLE_UTC = False
CELERY_TIMEZONE = "Europe/Kiev"
CELERY_QUEUE_HA_POLICY = 'all'
CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'application/text']
CELERYD_PREFETCH_MULTIPLIER = 1
CELERYD_CONCURRENCY = 2
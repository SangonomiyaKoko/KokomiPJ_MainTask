import eventlet
eventlet.monkey_patch() 
from celery import Celery, signals
from celery.app.base import logger

from app.core import EnvConfig
from app.db import DatabaseConnection

config = EnvConfig.get_config()

# 创建 Celery 应用
celery_app = Celery(
    "tasks",
    broker=f"pyamqp://{config.RABBITMQ_USERNAME}:{config.RABBITMQ_PASSWORD}@{config.RABBITMQ_HOST}//",  # RabbitMQ 连接地址
    backend="rpc://",  # 使用 RabbitMQ 作为结果存储
    include=['app.tasks'],
    broker_connection_retry_on_startup = True
)

# 配置 Celery
celery_app.conf.update(
    task_routes={
        "tasks.add": {"queue": "task_queue"}
    }
)

celery_app.conf.result_expires = 86400  # 设置任务结果过期时间为 24 小时（86400 秒）

# 初始化
@signals.worker_ready.connect
def init_app(**kwargs):
    DatabaseConnection.init_pool()
    logger.info('MySQL initialized')

# 释放资源
@signals.worker_shutdown.connect
def close_app(**kwargs):
    DatabaseConnection.close_pool()
    logger.info('MySQL closed')

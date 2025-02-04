from celery import Celery

from config import config

celery_app = Celery(
    'producer',
    broker=f"pyamqp://{config.RABBITMQ_USERNAME}:{config.RABBITMQ_PASSWORD}@{config.RABBITMQ_HOST}//"
)

result = celery_app.send_task(
    name='test',
    args=None,
    queue='task_queue'
)

print(result)

celery_app.close()
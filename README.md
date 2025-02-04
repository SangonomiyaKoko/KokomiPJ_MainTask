# Kokomi Task

这里是负责后台任务模块

Celery 文档: <https://docs.celeryq.dev/en/latest/index.html>

## 启动步骤

### 启动 Celery Worker

```bash
# 单个 worker
celery --app app.main:celery_app worker -P eventlet -Q task_queue --loglevel=debug

# 多个 worker
celery --app app.main:celery_app worker -P eventlet -Q task_queue --loglevel=info --hostname=worker1@%h
```

> `--hostname=worker1@%h` 和 `--hostname=worker2@%h` 是为了区分 Worker，避免 RabbitMQ 任务分配时发生冲突。

## 📌 为什么使用 eventlet

默认情况下，Celery 使用的是 prefork（多进程）模式，每个任务占用一个进程，进程切换开销较大。eventlet 采用协程来并发执行任务，任务可以在等待 I/O 时释放 CPU 资源，这样其他任务可以继续执行，提高并发效率。

> ⚠️ 如果你的任务是 CPU 密集型的，建议使用 --pool=threads 或 --pool=gevent。

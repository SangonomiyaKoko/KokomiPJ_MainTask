import pymysql
from dbutils.pooled_db import PooledDB

from app.core import EnvConfig

config = EnvConfig.get_config()

class DatabaseConnection:
    _pool = None

    @classmethod
    def init_pool(cls):
        try:
            cls._pool = PooledDB(
                creator=pymysql,
                maxconnections=2,  # 最大连接数
                mincached=1,        # 初始化时，连接池中至少创建的空闲的连接
                maxcached=2,        # 最大缓存的连接
                blocking=True,      # 连接池中如果没有可用连接后，是否阻塞
                host=config.MYSQL_HOST,
                user=config.MYSQL_USERNAME,
                password=config.MYSQL_PASSWORD,
                charset='utf8mb4',
                connect_timeout=10
            )
        except Exception as e:
            print(e)

    @classmethod
    def close_pool(cls):
        if cls._pool:
            cls._pool.close()
    
    @classmethod
    def get_pool(cls):
        if cls._pool:
            return cls._pool
        else:
            cls.init_pool()
            return cls._pool



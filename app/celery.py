import pymysql
from dbutils.pooled_db import PooledDB
from celery import Celery, signals
from celery.app.base import logger

from .db.background_task import (
    check_game_version,
    check_user_basic, 
    check_clan_basic, 
    check_user_info,
    check_user_recent,
    update_user_clan, 
    update_user_ship,
    update_user_ships,
    update_clan_users,
    check_clan_info,
    update_users_clan
)
from app.core import EnvConfig

config = EnvConfig.get_config()

# 创建 Celery 应用并配置 Redis 作为消息队列
celery_app = Celery(
    "worker",
    broker=f"redis://:{config.REDIS_PASSWORD}@{config.REDIS_HOST}:{config.REDIS_PORT}/1",  # 消息代理
    backend=f"redis://:{config.REDIS_PASSWORD}@{config.REDIS_HOST}:{config.REDIS_PORT}/2", # 结果存储
    broker_connection_retry_on_startup = True
)

# 创建连接池
pool = None


@signals.worker_init.connect
def init_mysql_pool(**kwargs):
    global pool
    pool = PooledDB(
        creator=pymysql,
        maxconnections=10,  # 最大连接数
        mincached=2,        # 初始化时，连接池中至少创建的空闲的连接
        maxcached=5,        # 最大缓存的连接
        blocking=True,      # 连接池中如果没有可用连接后，是否阻塞
        host=config.MYSQL_HOST,
        user=config.MYSQL_USERNAME,
        password=config.MYSQL_PASSWORD,
        charset='utf8mb4',
        connect_timeout=10
    )

@signals.worker_shutdown.connect
def close_mysql_pool(**kwargs):
    pool.close()
    logger.info('MySQL closed')


@celery_app.task(name="check_game_version")
def task_check_game_version(game_data: dict):
    "检查数据库中的version是否和当前接口的数据一致"
    result = check_game_version(pool,game_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'
    

@celery_app.task(name="check_user_basic")
def task_check_user_basic(user_data: dict):
    "检查user_basic表中用户名称是否改变"
    result = check_user_basic(pool,user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="check_clan_basic")
def task_check_clan_basic(clan_data: dict):
    "检查clan_basic表中工会名称和颜色是否改变，实际是更新"
    result = check_clan_basic(pool,clan_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'


@celery_app.task(name="check_clan_info")
def task_check_clan_info(clan_data: dict):
    "检查clan_info表中工会的当前数据是否改变"
    result = check_clan_info(pool,clan_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="check_clan_basic_and_info")
def task_check_clan_basic_and_info(clan_basic: dict, clan_info: dict):
    "结合上面两个的功能"
    result = check_clan_basic(pool,clan_basic)
    if result.get('code', None) != 1000:
        print(result)
    result = check_clan_info(pool,clan_info)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'
    
@celery_app.task(name="update_user_clan")
def task_update_user_clan(user_data: dict):
    "更新user_clan表中用户所在工会信息"
    result = update_user_clan(pool,user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="update_clan_and_user")
def task_update_clan_and_user(clan_data: dict, user_data: dict):
    "更新用户的工会和工会数据"
    result = check_clan_basic(pool,clan_data)
    if result.get('code', None) != 1000:
        print(result)
    result = update_user_clan(pool,user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="check_user_basic_and_info")
def task_check_user_basic_and_info(user_basic: dict, user_info: dict):
    "更新用户的名称和活跃数据"
    result = check_user_basic(pool,user_basic)
    if result.get('code', None) != 1000:
        print(result)
    result = check_user_info(pool,user_info)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

    
@celery_app.task(name="check_user_info")
def task_check_user_info(user_data: dict):
    "更新用户的活跃数据"
    result = check_user_info(pool,user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="update_user_cache")
def task_update_user_cache(ships_data: dict, ship_data: dict):
    "更新用户的船只缓存数据"
    if ship_data:
        result = update_user_ship(pool,ship_data)
        if result.get('code', None) != 1000:
            print(result)
            return 'error'
    if ships_data:
        result = update_user_ships(pool,ships_data)
        if result.get('code', None) != 1000:
            print(result)
            return 'error'
    return 'ok'

@celery_app.task(name="check_user_recent")
def task_check_user_recent(user_data: dict):
    "检查用户recent数据是否需要更新"
    result = check_user_recent(pool, user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="update_clan_users")
def task_update_clan_users(clan_id: int, hash_value: str, user_data: list):
    "更新工会的users数据"
    if len(user_data) != 0:
        result = update_users_clan(pool, clan_id, user_data)
        if result.get('code', None) != 1000:
            print(result)
            return 'error'
    result = update_clan_users(pool, clan_id, hash_value, user_data)
    if result.get('code', None) != 1000:
        print(result)
        return 'error'
    return 'ok'
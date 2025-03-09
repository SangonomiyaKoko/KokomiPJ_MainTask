from .main import celery_app
from .db import *

@celery_app.task(name="test")
def task_test():
    "测试"
    result = test_db()
    if result.get('code', None) != 1000:
        print(result)
    else:
        print('MySQL Version: ' + str(result['data']['version']))
    return 'ok'

@celery_app.task(name="update_user_data")
def task_update_user_data(user_datas: dict | list):
    """更新用户数据库的数据

    更新包括user_basic, user_info, user_clan数据表

    参数格式如下：
    user_data = {
        'region_id': None,
        'account_id': None,
        'basic': {
            'nickname': None
        },
        'info': {
            'is_active': None,
            'is_public': None,
            'total_battles': None,
            'last_battle_time': None
        },
        'clan': {
            'id': None,
            'tag': None,
            'league'
        }
    }
    如果某个数据没有，则value设置为None或者{}，建议统一使用None
    """
    result = update_user_data(user_datas)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'


@celery_app.task(name="update_clan_data")
def task_update_clan_data(clan_data: dict):
    """更新工会数据库的数据

    更新包括clan_basic, clan_info数据表

    参数格式如下：
    user_data = {
        'region_id': None,
        'clan_id': None,
        'basic': {
            'tag': None,
            'league': None
        },
        'info': {
            'is_active': None,
            'season_number': None,
            'public_rating': None,
            'league': None,
            'division': None,
            'division_rating': None,
            'last_battle_at': None
        }
    }
    如果某个数据没有，则value设置为None或者{}，建议统一使用None
    """
    result = update_clan_data(clan_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'
    
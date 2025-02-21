from .main import celery_app
from .utils import HashUtils
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

@celery_app.task(name="check_game_version")
def task_check_game_version(game_data: dict):
    "检查数据库中的version是否和当前接口的数据一致"
    result = check_game_version(game_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'
    

@celery_app.task(name="check_user_basic")
def task_check_user_basic(user_data: dict):
    "检查user_basic表中用户名称是否改变"
    result = check_user_basic(user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="check_clan_basic")
def task_check_clan_basic(clan_data: dict):
    "检查clan_basic表中工会名称和颜色是否改变，实际是更新"
    result = check_clan_basic(clan_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'


@celery_app.task(name="check_clan_info")
def task_check_clan_info(clan_data: dict):
    "检查clan_info表中工会的当前数据是否改变"
    result = check_clan_info(clan_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="check_clan_basic_and_info")
def task_check_clan_basic_and_info(clan_basic: dict, clan_info: dict):
    "结合上面两个的功能"
    result = check_clan_basic(clan_basic)
    if result.get('code', None) != 1000:
        print(result)
    result = check_clan_info(clan_info)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'
    
@celery_app.task(name="update_user_clan")
def task_update_user_clan(user_data: dict):
    "更新user_clan表中用户所在工会信息"
    result = update_user_clan(user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="update_clan_and_user")
def task_update_clan_and_user(clan_data: dict, user_data: dict):
    "更新用户的工会和工会数据"
    result = check_clan_basic(clan_data)
    if result.get('code', None) != 1000:
        print(result)
    result = update_user_clan(user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

@celery_app.task(name="check_user_basic_and_info")
def task_check_user_basic_and_info(user_basic: dict, user_info: dict):
    "更新用户的名称和活跃数据"
    result = check_user_basic(user_basic)
    if result.get('code', None) != 1000:
        print(result)
    result = check_user_info(user_info)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

# @celery_app.task(name="check_clan_basic_and_info")
# def task_check_clan_basic_and_info(clan_data: dict):
#     "更新工会basic和info信息，调用来源：ClanUsers功能"
#     # 更新clan_basic和clan_info表的信息
#     result = update_clan_basic_and_info(clan_data)
#     if result.get('code', None) != 1000:
#         print(result)
#     return 'ok'
    
@celery_app.task(name="check_user_info")
def task_check_user_info(user_data: dict):
    "更新用户的活跃数据"
    result = check_user_info(user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

# @celery_app.task(name="update_user_cache")
# def task_update_user_cache(ships_data: dict, ship_data: dict):
#     "更新用户的船只缓存数据"
#     if ship_data:
#         result = update_user_ship(ship_data)
#         if result.get('code', None) != 1000:
#             print(result)
#             return 'error'
#     if ships_data:
#         result = update_user_ships(ships_data)
#         if result.get('code', None) != 1000:
#             print(result)
#             return 'error'
#     return 'ok'

@celery_app.task(name="check_user_recent")
def task_check_user_recent(user_data: dict):
    "检查用户recent数据是否需要更新"
    result = check_user_recent(user_data)
    if result.get('code', None) != 1000:
        print(result)
    return 'ok'

# @celery_app.task(name="update_clan_users")
# def task_update_clan_users(clan_id: int, clan_users: list):
#     "更新工会的users数据，调用来源：ClanUsers功能"
#     # 首先检查传入的用户是否都在数据库中存在
#     result = check_and_insert_missing_users(clan_users)
#     if result.get('code', None) != 1000:
#         print(result)
#         return 'error'
#     # 批量更新用户达到user_clan表
#     user_data = []
#     for user in clan_users:
#         user_data.append(user[0])
#     if len(user_data) != 0:
#         result = update_users_clan(clan_id, user_data)
#         if result.get('code', None) != 1000:
#             print(result)
#             return 'error'
#     # 最后更新工会内所有用户的数据
#     hash_value = HashUtils.get_clan_users_hash(user_data)
#     result = update_clan_users(clan_id, hash_value, user_data)
#     if result.get('code', None) != 1000:
#         print(result)
#         return 'error'
#     return 'ok'
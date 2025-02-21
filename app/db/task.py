import pymysql
from dbutils.pooled_db import PooledDB

from app.response import JSONResponse
from app.log import ExceptionLogger
from app.utils import BinaryGeneratorUtils, BinaryParserUtils, TimeFormat
from app.core import EnvConfig

from .db import DatabaseConnection

config = EnvConfig.get_config()

MAIN_DB = config.DB_NAME_MAIN
BOT_DB = config.DB_NAME_BOT
CACHE_DB = config.DB_NAME_SHIP


@ExceptionLogger.handle_database_exception_sync
def test_db():
    '''测试'''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        
        cur.execute(
            "SELECT VERSION();"
        )
        version = cur.fetchone()
        data = {
            'version': version['version']
        }
        
        conn.commit()
        return JSONResponse.get_success_response(data)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        conn.close()

# @ExceptionLogger.handle_database_exception_sync
# def check_game_version(game_data: dict):
#     '''检查游戏版本是否更改
    
#     参数:
#         game_data
#     '''
#     pool = DatabaseConnection.get_pool()
#     conn = pool.connection()
#     cur = None
#     try:
#         conn.begin()
#         cur = conn.cursor(pymysql.cursors.DictCursor)
        
#         region_id = game_data['region_id']
#         game_version = game_data['version']
#         cur.execute(
#             f"SELECT game_version FROM {MAIN_DB}.region_version WHERE region_id = %s;",
#             [region_id]
#         )
#         game = cur.fetchone()
#         if game == None:
#             raise ValueError('Table Not Found')
#         else:
#             if game['game_version'] != game_version:
#                 cur.execute(
#                     f"UPDATE {MAIN_DB}.region_version SET game_version = %s WHERE region_id = %s;",
#                     [game_version, region_id]
#                 )
        
#         conn.commit()
#         return JSONResponse.API_1000_Success
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         if cur:
#             cur.close()
#         conn.close()

@ExceptionLogger.handle_database_exception_sync
def check_user_basic(user_data: dict):
    '''检查用户数据是否需要更新

    参数:
        user_list [dict]
    '''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        account_id = user_data['account_id']
        region_id = user_data['region_id']
        nickname = user_data['nickname']
        cur.execute(
            "SELECT username, UNIX_TIMESTAMP(updated_at) AS update_time "
            f"FROM {MAIN_DB}.user_basic WHERE region_id = %s and account_id = %s;", 
            [region_id, account_id]
        )
        user = cur.fetchone()
        if not user:
            cur.execute(
                f"INSERT INTO {MAIN_DB}.user_basic (account_id, region_id, username) VALUES (%s, %s, %s);",
                [account_id, region_id, f'User_{account_id}']
            )
            cur.execute(
                f"INSERT INTO {MAIN_DB}.user_info (account_id) VALUES (%s);",
                [account_id]
            )
            cur.execute(
                f"INSERT INTO {MAIN_DB}.user_ships (account_id) VALUES (%s);",
                [account_id]
            )
            cur.execute(
                f"INSERT INTO {MAIN_DB}.user_clan (account_id) VALUES (%s);",
                [account_id]
            )
            cur.execute(
                f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s AND account_id = %s",
                [nickname, region_id, account_id]
            )
        else:
            # 根据数据库的数据判断用户是否更改名称
            if user['username'] != nickname and user['update_time'] != None:
                cur.execute(
                    f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s and account_id = %s;", 
                    [nickname, region_id, account_id]
                )
                cur.execute(
                    f"INSERT INTO {MAIN_DB}.user_history (account_id, username, start_time, end_time) VALUES "
                    "(%s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s));", 
                    [account_id, user['username'], user['update_time'], TimeFormat.get_current_timestamp()]
                )
            elif user['update_time'] == None:
                cur.execute(
                    f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s and account_id = %s;",
                    [nickname, region_id, account_id]
                )
        
        conn.commit()
        return JSONResponse.API_1000_Success
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        conn.close()

@ExceptionLogger.handle_database_exception_sync
def check_clan_basic(clan_data: dict):
    '''检查clan_basic是否需要更新

    参数：
        clan_list [clan_id,region_id,tag,league]
    '''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        clan_id = clan_data['clan_id']
        region_id = clan_data['region_id']
        tag = clan_data['tag']
        league = clan_data['league']
        cur.execute(
            f"SELECT tag, league FROM {MAIN_DB}.clan_basic WHERE region_id = %s and clan_id = %s;", 
            [region_id, clan_id]
        )
        clan = cur.fetchone()
        if clan is None:
            # 工会不存在，插入新数据
            cur.execute(
                f"INSERT INTO {MAIN_DB}.clan_basic (clan_id, region_id, tag, league) VALUES (%s, %s, %s, %s);", 
                [clan_id, region_id, 'N/A', 5]
            )
            cur.execute(
                f"INSERT INTO {MAIN_DB}.clan_info (clan_id) VALUES (%s);", 
                [clan_id]
            )
            cur.execute(
                f"INSERT INTO {MAIN_DB}.clan_users (clan_id) VALUES (%s);", 
                [clan_id]
            )
            cur.execute(
                f"INSERT INTO {MAIN_DB}.clan_season (clan_id) VALUES (%s);", 
                [clan_id]
            )
            cur.execute(
                f"UPDATE {MAIN_DB}.clan_basic SET tag = %s, league = %s WHERE region_id = %s and clan_id = %s;",
                [tag, league, region_id, clan_id]
            )
        else:
            cur.execute(
                f"UPDATE {MAIN_DB}.clan_basic SET tag = %s, league = %s, updated_at = CURRENT_TIMESTAMP "
                "WHERE region_id = %s and clan_id = %s;",
                [tag, league, region_id, clan_id]
            )
        
        conn.commit()
        return JSONResponse.API_1000_Success
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        conn.close()

@ExceptionLogger.handle_database_exception_sync
def check_user_info(user_data: dict):
    '''检查并更新user_info表

    参数:
        user_list [dict]
    
    '''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        account_id = user_data['account_id']
        cur.execute(
            "SELECT is_active, active_level, is_public, total_battles, UNIX_TIMESTAMP(last_battle_at) AS last_battle_time "
            f"FROM {MAIN_DB}.user_info WHERE account_id = %s;", 
            [account_id]
        )
        user = cur.fetchone()
        if user is None:
            # 正常来说这里不应该会遇到为空问题，因为先检查basic在检查info
            conn.commit()
            return {'status': 'ok','code': 1008,'message': 'UserNotExistinDatabase','data' : None}
        sql_str = ''
        params = []
        for field in ['is_active', 'active_level', 'is_public', 'total_battles', 'last_battle_time']:
            if (user_data[field] != None) and (user_data[field] != user[field]):
                if field != 'last_battle_time':
                    sql_str += f'{field} = %s, '
                    params.append(user_data[field])
                else:
                    if user_data[field] != 0:
                        sql_str += f'last_battle_at = FROM_UNIXTIME(%s), '
                        params.append(user_data[field])
        params = params + [account_id]
        cur.execute(
            f"UPDATE {MAIN_DB}.user_info SET {sql_str}updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;", 
            params
        )

        conn.commit()
        return JSONResponse.API_1000_Success
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        conn.close()

# @ExceptionLogger.handle_database_exception_sync
# def update_clan_basic_and_info(clan_data: dict):
#     '''更新clan_info表

#     更新工会的info数据

#     参数:
#         clan_data
    
#     返回:
#         ResponseDict
#     '''
#     pool = DatabaseConnection.get_pool()
#     conn = pool.connection()
#     cur = None
#     try:
#         conn.begin()
#         cur = conn.cursor(pymysql.cursors.DictCursor)

#         clan_id = clan_data['clan_id']
#         region_id = clan_data['region_id']
#         cur.execute(
#             "SELECT b.clan_id, b.tag, b.league, UNIX_TIMESTAMP(b.updated_at) AS basic_update_time, "
#             "i.is_active, i.season, i.public_rating, i.league, i.division, i.division_rating, "
#             "UNIX_TIMESTAMP(i.last_battle_at) AS info_last_battle_time "
#             f"FROM {MAIN_DB}.clan_basic AS b "
#             f"LEFT JOIN {MAIN_DB}.clan_info AS i ON b.clan_id = i.clan_id "
#             "WHERE b.region_id = %s AND b.clan_id = %s;",
#             [region_id, clan_id]
#         )
#         clan = cur.fetchone()
#         if clan == None:
#             conn.commit()
#             return {'status': 'ok','code': 1009,'message': 'ClanNotExistinDatabase','data' : None}
#         if clan_data['is_active'] == 0:
#             cur.execute(
#                 f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, updated_at = CURRENT_TIMESTAMP WHERE clan_id = %s;",
#                 [0, clan_id]
#             )
#         else:
#             current_timestamp = TimeFormat.get_current_timestamp()
#             if (
#                 not clan[3] or 
#                 current_timestamp - clan['basic_update_time'] > 2*24*60*60 or
#                 (clan_data['tag'] and clan_data['tag'] != clan['b.tag']) or
#                 clan_data['league'] != clan['b.league']
#             ):
#                 cur.execute(
#                     f"UPDATE {MAIN_DB}.clan_basic SET tag = %s, league = %s, updated_at = CURRENT_TIMESTAMP "
#                     "WHERE region_id = %s AND clan_id = %s;",
#                     [clan_data['tag'],clan_data['league'],region_id,clan_id]
#                 )
#             if (
#                 clan_data['season_number'] != clan['i.season'] or
#                 clan_data['public_rating'] != clan['i.public_rating'] or
#                 clan_data['last_battle_at'] != clan['info_last_battle_time']
#             ):
#                 cur.execute(
#                     f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, season = %s, public_rating = %s, league = %s, "
#                     "division = %s, division_rating = %s, last_battle_at = FROM_UNIXTIME(%s) "
#                     "WHERE clan_id = %s",
#                     [
#                         1, clan_data['season_number'], clan_data['public_rating'],clan_data['league'],
#                         clan_data['division'], clan_data['division_rating'],clan_data['last_battle_at'],clan_id
#                     ]
#                 )

#         conn.commit()
#         return JSONResponse.API_1000_Success
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         if cur:
#             cur.close()
#         conn.close()

@ExceptionLogger.handle_database_exception_sync
def check_clan_info(clan_data: dict):
    '''检查并更新clan_info表

    参数:
        clan_data [dict]
    
    '''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        clan_id = clan_data['clan_id']
        cur.execute(
            "SELECT is_active, season, public_rating, league, division, division_rating, "
            "UNIX_TIMESTAMP(last_battle_at) AS last_battle_time "
            f"FROM {MAIN_DB}.clan_info WHERE clan_id = %s;", 
            [clan_id]
        )
        clan = cur.fetchone()
        if clan is None:
            # 正常来说这里不应该会遇到为空问题，因为先检查basic在检查info
            conn.commit()
            return {'status': 'ok','code': 1009,'message': 'ClanNotExistinDatabase','data' : None}
        if clan_data['is_active'] == False:
            cur.execute(
                f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, updated_at = CURRENT_TIMESTAMP WHERE clan_id = %s;",
                [0, clan_id]
            )
        else:
            if (
                clan_data['season_number'] != clan['season'] or
                clan_data['public_rating'] != clan['public_rating'] or
                clan_data['last_battle_at'] != clan['last_battle_time']
            ):
                if clan_data['last_battle_at']:
                    cur.execute(
                        f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, season = %s, public_rating = %s, league = %s, "
                        "division = %s, division_rating = %s, last_battle_at = FROM_UNIXTIME(%s) "
                        "WHERE clan_id = %s",
                        [
                            1, clan_data['season_number'], clan_data['public_rating'],clan_data['league'],clan_data['division'],
                            clan_data['division_rating'],clan_data['last_battle_at'],clan_id
                        ]
                    )
                else:
                    cur.execute(
                        f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, season = %s, public_rating = %s, league = %s, "
                        "division = %s, division_rating = %s "
                        "WHERE clan_id = %s",
                        [
                            1, clan_data['season_number'], clan_data['public_rating'],clan_data['league'],
                            clan_data['division'], clan_data['division_rating'],clan_id
                        ]
                    )

        conn.commit()
        return JSONResponse.API_1000_Success
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        conn.close()

@ExceptionLogger.handle_database_exception_sync
def check_user_recent(user_data: dict):
    '''检查并更新recent表

    参数:
        user_data [dict]
    '''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        account_id = user_data['account_id']
        region_id = user_data['region_id']
        cur.execute(
            "SELECT recent_class, UNIX_TIMESTAMP(last_update_at) AS last_update_time "
            f"FROM {MAIN_DB}.recent WHERE region_id = %s and account_id = %s;", 
            [region_id, account_id]
        )
        user = cur.fetchone()
        if user == None:
            conn.commit()
            return JSONResponse.API_1000_Success
        if user_data['recent_class'] and (user['recent_class'] != user_data['recent_class']):
            cur.execute(
                f"UPDATE {MAIN_DB}.recent SET recent_class = %s WHERE region_id = %s and account_id = %s;",
                [user_data['recent_class'], region_id, account_id]
            )
        if not user['last_update_time']:
            cur.execute(
                f"UPDATE {MAIN_DB}.recent SET last_update_at = FROM_UNIXTIME(%s) WHERE region_id = %s and account_id = %s;",
                [user_data['last_update_time'], region_id, account_id]
            )
        elif user_data['last_update_time'] and (user['last_update_time'] > user_data['last_update_time']):
            cur.execute(
                f"UPDATE {MAIN_DB}.recent SET last_update_at = FROM_UNIXTIME(%s) WHERE region_id = %s and account_id = %s;",
                [user_data['last_update_time'], region_id, account_id]
            )
        
        conn.commit()
        return JSONResponse.API_1000_Success
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        conn.close()

# @ExceptionLogger.handle_database_exception_sync
# def update_user_ship(user_data: dict):
#     '''检查并更新user_ship表

#     参数:
#         user_data [dict]
#     '''
#     pool = DatabaseConnection.get_pool()
#     conn = pool.connection()
#     cur = None
#     try:
#         conn.begin()
#         cur = conn.cursor(pymysql.cursors.DictCursor)
        
#         account_id = user_data['account_id']
#         region_id = user_data['region_id']
#         delete_ship_list = user_data['delete_ship_list']
#         replace_ship_dict = user_data['replace_ship_dict']
#         for del_ship_id in delete_ship_list:
#             cur.execute(
#                 f"DELETE FROM {CACHE_DB}.ship_%s "
#                 "WHERE region_id = %s AND account_id = %s;",
#                 [int(del_ship_id), region_id, account_id]
#             )
#         for update_ship_id, ship_data in replace_ship_dict.items():
#             cur.execute(
#                 f"UPDATE {CACHE_DB}.ship_%s SET battles_count = %s, battle_type_1 = %s, battle_type_2 = %s, battle_type_3 = %s, wins = %s, "
#                 "damage_dealt = %s, frags = %s, exp = %s, survived = %s, scouting_damage = %s, art_agro = %s, "
#                 "planes_killed = %s, max_exp = %s, max_damage_dealt = %s, max_frags = %s "
#                 "WHERE region_id = %s AND account_id = %s;",
#                 [int(update_ship_id)] + ship_data + [region_id, account_id]
#             )
#             cur.execute(
#                 f"INSERT INTO {CACHE_DB}.ship_%s (account_id, region_id, battles_count, battle_type_1, battle_type_2, "
#                 "battle_type_3, wins, damage_dealt, frags, exp, survived, scouting_damage, art_agro, planes_killed, "
#                 "max_exp, max_damage_dealt, max_frags) "
#                 "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s "
#                 f"WHERE NOT EXISTS (SELECT 1 FROM {CACHE_DB}.ship_%s WHERE region_id = %s AND account_id = %s);",
#                 [int(update_ship_id)] + [account_id, region_id] + ship_data + [int(update_ship_id)] + [region_id, account_id]
#             )
        
#         conn.commit()
#         return JSONResponse.API_1000_Success
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         if cur:
#             cur.close()
#         conn.close()

# @ExceptionLogger.handle_database_exception_sync
# def check_and_insert_missing_users(users: list):
#     '''检查并插入缺失的用户id

#     只支持同一服务器下的用户
    
#     参数:
#         user: [{...}]
#     '''
#     pool = DatabaseConnection.get_pool()
#     conn = pool.connection()
#     cur = None
#     try:
#         conn.begin()
#         cur = conn.cursor(pymysql.cursors.DictCursor)

#         sql_str = ''
#         params = [users[0]['region_id'],users[0]['account_id']]
#         for user in users[1:]:
#             sql_str += ', %s'
#             params.append(user['account_id'])
#         cur.execute(
#             "SELECT account_id, username, UNIX_TIMESTAMP(updated_at) AS update_time "
#             f"FROM {MAIN_DB}.user_basic WHERE region_id = %s AND account_id in ( %s{sql_str} );",
#             params
#         )
#         exists_users = {}
#         rows = cur.fetchall()
#         for row in rows:
#             exists_users[row['account_id']] = [row['username'],row['update_time']]
#         for user in users:
#             account_id = user[0]
#             region_id = user[1]
#             nickname = user[2]
#             if account_id not in exists_users:
#                 cur.execute(
#                     f"INSERT INTO {MAIN_DB}.user_basic (account_id, region_id, username) VALUES (%s, %s, %s);",
#                     [account_id, region_id, f'User_{account_id}']
#                 )
#                 cur.execute(
#                     f"INSERT INTO {MAIN_DB}.user_info (account_id) VALUES (%s);",
#                     [account_id]
#                 )
#                 cur.execute(
#                     f"INSERT INTO {MAIN_DB}.user_ships (account_id) VALUES (%s);",
#                     [account_id]
#                 )
#                 cur.execute(
#                     f"INSERT INTO {MAIN_DB}.user_clan (account_id) VALUES (%s);",
#                     [account_id]
#                 )
#                 cur.execute(
#                     f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s AND account_id = %s",
#                     [nickname, region_id, account_id]
#                 )
#             else:
#                 if exists_users[account_id][1] == None:
#                     cur.execute(
#                         f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s AND account_id = %s",
#                         [nickname, region_id, account_id]
#                     )
#                 elif nickname != exists_users[account_id][0]:
#                     cur.execute(
#                         f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s and account_id = %s;", 
#                         [nickname, region_id, account_id]
#                     ) 
#                     cur.execute(
#                         f"INSERT INTO {MAIN_DB}.user_history (account_id, username, start_time, end_time) "
#                         "VALUES (%s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s));", 
#                         [account_id, exists_users[account_id][0], exists_users[account_id][1], TimeFormat.get_current_timestamp()]
#                     )
        
#         conn.commit()
#         return JSONResponse.API_1000_Success
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         if cur:
#             cur.close()
#         conn.close()

# @ExceptionLogger.handle_database_exception_sync
# def update_clan_users(clan_id: int, hash_value: str, user_data: list):
#     '''更新clan_users表'''
#     pool = DatabaseConnection.get_pool()
#     conn = pool.connection()
#     cur = None
#     try:
#         conn.begin()
#         cur = conn.cursor(pymysql.cursors.DictCursor)

#         cur.execute(
#             "SELECT hash_value, users_data, users_data, UNIX_TIMESTAMP(updated_at) AS update_time "
#             f"FROM {MAIN_DB}.clan_users WHERE clan_id = %s;", 
#             [clan_id]
#         )
#         clan = cur.fetchone()
#         if clan is None:
#             conn.commit()
#             return {'status': 'ok','code': 1009,'message': 'ClanNotExistinDatabase','data' : None}
#         # 判断是否有工会人员变动
#         join_user_list = []
#         leave_user_list = []
#         if clan['update_time'] and clan['hash_value'] != hash_value:
#             old_user_list = BinaryParserUtils.from_clan_binary_data_to_list(clan['users_data'])
#             for account_id in user_data:
#                 if account_id not in old_user_list:
#                     join_user_list.append(account_id)
#             for account_id in old_user_list:
#                 if account_id not in user_data:
#                     leave_user_list.append(account_id)
#         cur.execute(
#             f"UPDATE {MAIN_DB}.clan_users "
#             "SET hash_value = %s, users_data = %s, updated_at = CURRENT_TIMESTAMP "
#             "WHERE clan_id = %s",
#             [hash_value, BinaryGeneratorUtils.to_clan_binary_data_from_list(user_data),clan_id]
#         )
#         for account_id in join_user_list:
#             cur.execute(
#                 f"INSERT INTO {MAIN_DB}.clan_history (account_id, clan_id, action_type) VALUES (%s, %s, %s);",
#                 [account_id, clan_id, 1]
#             )
#         for account_id in leave_user_list:
#             cur.execute(
#                 f"INSERT INTO {MAIN_DB}.clan_history (account_id, clan_id, action_type) VALUES (%s, %s, %s);",
#                 [account_id, clan_id, 2]
#             )
        
#         conn.commit()
#         return JSONResponse.API_1000_Success
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         if cur:
#             cur.close()
#         conn.close()

# @ExceptionLogger.handle_database_exception_sync
# def update_users_clan(clan_id: int, user_data: list):
#     '''更新user_clan表'''
#     pool = DatabaseConnection.get_pool()
#     conn = pool.connection()
#     cur = None
#     try:
#         conn.begin()
#         cur = conn.cursor(pymysql.cursors.DictCursor)

#         sql_str = ''
#         params = []
#         for aid in user_data[1:]:
#             sql_str += ', %s'
#             params.append(aid)
#         cur.execute(
#             f"UPDATE {MAIN_DB}.user_clan "
#             "SET clan_id = %s, updated_at = CURRENT_TIMESTAMP "
#             f"WHERE account_id IN ( %s{sql_str} );", 
#             [clan_id] + [user_data[0]] + params
#         )

#         conn.commit()
#         return JSONResponse.API_1000_Success
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         if cur:
#             cur.close()
#         conn.close()

# @ExceptionLogger.handle_database_exception_sync
# def update_user_ships(user_data: dict):
#     '''检查并更新user_ships表

#     参数:
#         user_data [dict]
#     '''
#     pool = DatabaseConnection.get_pool()
#     conn = pool.connection()
#     cur = None
#     try:
#         conn.begin()
#         cur = conn.cursor(pymysql.cursors.DictCursor)
        
#         account_id = user_data['account_id']
#         if user_data['hash_value']:
#             cur.execute(
#                 f"UPDATE {MAIN_DB}.user_ships "
#                 "SET battles_count = %s, hash_value = %s, ships_data = %s, updated_at = CURRENT_TIMESTAMP "
#                 "WHERE account_id = %s;", 
#                 [
#                     user_data['battles_count'], 
#                     user_data['hash_value'], 
#                     BinaryGeneratorUtils.to_user_binary_data_from_dict(user_data['ships_data']), 
#                     account_id
#                 ]
#             )
#         else:
#             cur.execute(
#                 f"UPDATE {MAIN_DB}.user_ships "
#                 "SET battles_count = %s, updated_at = CURRENT_TIMESTAMP "
#                 "WHERE account_id = %s;", 
#                 [user_data['battles_count'], account_id]
#             )

#         conn.commit()
#         return JSONResponse.API_1000_Success
#     except Exception as e:
#         conn.rollback()
#         raise e
#     finally:
#         if cur:
#             cur.close()
#         conn.close()

@ExceptionLogger.handle_database_exception_sync
def update_user_clan(user_data: dict):
    '''更新user_clan表

    此函数不能用来批量更新数据

    参数：
        user_clan_list [account_id,clan_id]
    '''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        account_id = user_data['account_id']
        clan_id = user_data['clan_id']
        if clan_id:
            cur.execute(
                f"UPDATE {MAIN_DB}.user_clan SET clan_id = %s, updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;",
                [clan_id, account_id]
            )
        else:
            cur.execute(
                f"UPDATE {MAIN_DB}.user_clan SET clan_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;",
                [account_id]
            )

        conn.commit()
        return JSONResponse.API_1000_Success
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if cur:
            cur.close()
        conn.close()
import pymysql
from dbutils.pooled_db import PooledDB

from app.response import JSONResponse
from app.log import ExceptionLogger
from app.utils import CommonUtils, TimeFormat
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


@ExceptionLogger.handle_database_exception_sync
def update_user_data(user_datas: dict | list):
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

        if type(user_datas) == dict:
            user_datas = [user_datas]
        for user_data in user_datas:
            account_id = user_data['account_id']
            region_id = user_data['region_id']
            cur.execute(
                "SELECT b.username, UNIX_TIMESTAMP(b.updated_at) AS name_update_time, "
                "i.is_active, i.active_level, i.is_public, i.total_battles, "
                "UNIX_TIMESTAMP(i.last_battle_at) AS last_battle_time, UNIX_TIMESTAMP(i.updated_at) AS info_update_time "
                f"FROM {MAIN_DB}.user_basic as b "
                f"LEFT JOIN {MAIN_DB}.user_info as i ON b.account_id = i.account_id "
                "WHERE b.region_id = %s and b.account_id = %s;", 
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
                # 更新user_basic表
                if user_data['basic'] != None and user_data['basic'] != {}:
                    cur.execute(
                        f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s AND account_id = %s;",
                        [user_data['basic']['nickname'], region_id, account_id]
                    )
                # 更新user_info表
                if user_data['info'] != None and user_data['info'] != {}:
                    if not user_data['info']['is_active']:
                        cur.execute(
                            f"UPDATE {MAIN_DB}.user_info SET is_active = %s WHERE account_id = %s;",
                            [user_data['info']['is_active'], account_id]
                        )
                    else:
                        active_level = CommonUtils.get_active_level(
                            is_public=user_data['info']['is_public'],
                            total_battles=user_data['info']['total_battles'],
                            last_battle_time=user_data['info']['last_battle_time']
                        )
                        cur.execute(
                            f"UPDATE {MAIN_DB}.user_info SET is_active = %s, active_level = %s, is_public = %s, "
                            "total_battles = %s, last_battle_at = FROM_UNIXTIME(%s) WHERE account_id = %s;",
                            [
                                user_data['info']['is_active'], 
                                active_level,
                                user_data['info']['is_public'], 
                                user_data['info']['total_battles'], 
                                user_data['info']['last_battle_time'], 
                                account_id
                            ]
                        )
            else:
                # 更新user_basic表
                if user_data['basic'] != None and user_data['basic'] != {}:
                    # 根据数据库的数据判断用户是否更改名称
                    if user['username'] != user_data['basic']['nickname'] and user['name_update_time'] != None:
                        cur.execute(
                            f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s and account_id = %s;", 
                            [user_data['basic']['nickname'], region_id, account_id]
                        )
                        cur.execute(
                            f"INSERT INTO {MAIN_DB}.user_history (account_id, username, start_time, end_time) VALUES "
                            "(%s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s));", 
                            [account_id, user['username'], user['name_update_time'], TimeFormat.get_current_timestamp()]
                        )
                    elif user['name_update_time'] == None:
                        cur.execute(
                            f"UPDATE {MAIN_DB}.user_basic SET username = %s WHERE region_id = %s and account_id = %s;",
                            [user_data['basic']['nickname'], region_id, account_id]
                        )
                # 更新user_info表
                if user_data['info'] != None and user_data['info'] != {}:
                    sql_str = ''
                    params = []
                    if user_data['info']['is_active']:
                        active_level = CommonUtils.get_active_level(
                            is_public=user_data['info']['is_public'],
                            total_battles=user_data['info']['total_battles'],
                            last_battle_time=user_data['info']['last_battle_time']
                        )
                        user_data['info']['active_level'] = active_level
                    for field in ['is_active', 'active_level', 'is_public', 'total_battles', 'last_battle_time']:
                        if (field in user_data['info']) and (user_data['info'][field] != None) and (user_data['info'][field] != user[field]):
                            if field != 'last_battle_time':
                                sql_str += f'{field} = %s, '
                                params.append(user_data['info'][field])
                            else:
                                if user_data['info'][field] != 0:
                                    sql_str += f'last_battle_at = FROM_UNIXTIME(%s), '
                                    params.append(user_data['info'][field])
                    params = params + [account_id]
                    cur.execute(
                        f"UPDATE {MAIN_DB}.user_info SET {sql_str}updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;", 
                        params
                    )
            if user_data['clan'] != None and user_data['clan'] != {}:
                if not user_data['clan']['id']:
                    cur.execute(
                        f"UPDATE {MAIN_DB}.user_clan SET clan_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;",
                        [account_id]
                    )
                else:
                    clan_id = user_data['clan']['id']
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
                            [user_data['clan']['tag'], user_data['clan']['league'], region_id, clan_id]
                        )
                    else:
                        cur.execute(
                            f"UPDATE {MAIN_DB}.clan_basic SET tag = %s, league = %s, updated_at = CURRENT_TIMESTAMP "
                            "WHERE region_id = %s and clan_id = %s;",
                            [user_data['clan']['tag'], user_data['clan']['league'], region_id, clan_id]
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
def update_clan_data(clan_datas: dict | list):
    '''更新clan_info表

    更新工会的info数据

    参数:
        clan_data
    
    返回:
        ResponseDict
    '''
    pool = DatabaseConnection.get_pool()
    conn = pool.connection()
    cur = None
    try:
        conn.begin()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        if type(clan_datas) == dict:
            clan_datas = [clan_datas]
        for clan_data in clan_datas:
            clan_id = clan_data['clan_id']
            region_id = clan_data['region_id']
            cur.execute(
                "SELECT b.clan_id, b.tag, b.league AS league1, UNIX_TIMESTAMP(b.updated_at) AS basic_update_time, "
                "i.is_active, i.season, i.public_rating, i.league, i.division, i.division_rating, "
                "UNIX_TIMESTAMP(i.last_battle_at) AS info_last_battle_time "
                f"FROM {MAIN_DB}.clan_basic AS b "
                f"LEFT JOIN {MAIN_DB}.clan_info AS i ON b.clan_id = i.clan_id "
                "WHERE b.region_id = %s AND b.clan_id = %s;",
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
                # 更新clan_basic表
                if clan_data['basic'] != None and clan_data['basic'] != {}:
                    cur.execute(
                        f"UPDATE {MAIN_DB}.clan_basic SET tag = %s, league = %s WHERE region_id = %s and clan_id = %s;",
                        [clan_data['basic']['tag'], clan_data['basic']['league'], region_id, clan_id]
                    )
                # 更新clan_info表
                if clan_data['info'] != None and clan_data['info'] != {}:
                    if not clan_data['info']['is_active']:
                        cur.execute(
                            f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, updated_at = CURRENT_TIMESTAMP WHERE clan_id = %s;",
                            [clan_data['info']['is_active'], clan_id]
                        )
                    else:
                        cur.execute(
                            f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, season = %s, public_rating = %s, league = %s, "
                            "division = %s, division_rating = %s, last_battle_at = FROM_UNIXTIME(%s) "
                            "WHERE clan_id = %s",
                            [
                                clan_data['info']['is_active'], clan_data['info']['season_number'], clan_data['info']['public_rating'],
                                clan_data['info']['league'], clan_data['info']['division'], clan_data['info']['division_rating'], 
                                clan_data['info']['last_battle_at'], clan_id
                            ]
                        )
            else:
                # 更新clan_basic表
                if clan_data['basic'] != None and clan_data['basic'] != {}:
                    if (
                        clan_data['basic']['tag'] != clan['tag'] or
                        clan_data['basic']['league'] != clan['league1']
                    ):
                        cur.execute(
                            f"UPDATE {MAIN_DB}.clan_basic SET tag = %s, league = %s, updated_at = CURRENT_TIMESTAMP WHERE region_id = %s and clan_id = %s;",
                            [clan_data['basic']['tag'], clan_data['basic']['league'], region_id, clan_id]
                        )
                # 更新clan_info表
                if clan_data['info'] != None and clan_data['info'] != {}:
                    if not clan_data['info']['is_active']:
                        cur.execute(
                            f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, updated_at = CURRENT_TIMESTAMP WHERE clan_id = %s;",
                            [clan_data['info']['is_active'], clan_id]
                        )
                    elif (
                        clan_data['info']['season_number'] != clan['season'] or
                        clan_data['info']['public_rating'] != clan['public_rating'] or
                        clan_data['info']['last_battle_at'] != clan['info_last_battle_time']
                    ):
                        cur.execute(
                            f"UPDATE {MAIN_DB}.clan_info SET is_active = %s, season = %s, public_rating = %s, league = %s, "
                            "division = %s, division_rating = %s, last_battle_at = FROM_UNIXTIME(%s) "
                            "WHERE clan_id = %s",
                            [
                                clan_data['info']['is_active'], clan_data['info']['season_number'], clan_data['info']['public_rating'],
                                clan_data['info']['league'], clan_data['info']['division'], clan_data['info']['division_rating'], 
                                clan_data['info']['last_battle_at'], clan_id
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

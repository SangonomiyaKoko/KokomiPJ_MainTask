import uuid
import traceback

import pymysql
import sqlite3

from .error_log import write_error_info
from app.response import JSONResponse

class ExceptionType:
    program = 'program'
    mysql = 'MySQL'

class DatabaseExceptionName:
    programming_error = 'ProgrammingError'
    operational_error = 'OperationalError'
    integrity_error = 'IntegrityError'
    database_error = 'DatabaseError'

def generate_error_id():
    return str(uuid.uuid4())

class ExceptionLogger:
    @staticmethod
    def handle_program_exception_async(func):
        "负责异步程序异常信息的捕获"
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.program,
                    error_name = str(type(e).__name__),
                    error_args = str(args) + str(kwargs),
                    error_info = traceback.format_exc()
                )
                return JSONResponse.get_error_response(5000,'ProgramError',error_id)
        return wrapper
    
    @staticmethod
    def handle_program_exception_sync(func):
        "负责同步程序异常信息的捕获"
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.program,
                    error_name = str(type(e).__name__),
                    error_args = str(args) + str(kwargs),
                    error_info = traceback.format_exc()
                )
                return JSONResponse.get_error_response(5000,'ProgramError',error_id)
        return wrapper
    
    @staticmethod
    def handle_database_exception_sync(func):
        "负责同步数据库 pymysql 和 sqlite3 的异常捕获"
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except pymysql.err.ProgrammingError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.programming_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3001,'DatabaseError',error_id)
            except pymysql.err.OperationalError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.operational_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3002,'DatabaseError',error_id)
            except pymysql.err.IntegrityError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.integrity_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3003,'DatabaseError',error_id)
            except pymysql.err.DatabaseError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.database_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3000,'DatabaseError',error_id)
            except sqlite3.ProgrammingError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.programming_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3001,'DatabaseError',error_id)
            except sqlite3.OperationalError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.operational_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3002,'DatabaseError',error_id)
            except sqlite3.IntegrityError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.integrity_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3003,'DatabaseError',error_id)
            except sqlite3.DatabaseError as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.mysql,
                    error_name = DatabaseExceptionName.database_error,
                    error_args = str(args) + str(kwargs),
                    error_info = f'ERROR_{e.args[0]}\n' + str(e.args[1]) + f'\n{traceback.format_exc()}'
                )
                return JSONResponse.get_error_response(3000,'DatabaseError',error_id)
            except Exception as e:
                error_id = generate_error_id()
                write_error_info(
                    error_id = error_id,
                    error_type = ExceptionType.program,
                    error_name = str(type(e).__name__),
                    error_info = traceback.format_exc()
                )
                return JSONResponse.get_error_response(5000,'ProgramError',error_id)
        return wrapper
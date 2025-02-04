from typing import Optional, Literal, Union, Any, Dict, List
from typing_extensions import TypedDict


class ResponseDict(TypedDict):
    '''返回数据格式'''
    status: Literal['ok', 'error']
    code: int
    message: str
    data: Optional[Union[Dict, List]]

class JSONResponse:
    '''接口返回值
    
    对于code是1000 2000~2003 3000 4000 5000的返回值，请使用内置函数获取response

    对于返回值的描述，请查看设计文档
    '''
    API_1000_Success = {'status': 'ok','code': 1000,'message': 'Success','data': None}

    # API_1_ = {'status': 'ok','code': 1,'message': '','data': None}


    @staticmethod
    def get_success_response(
        data: Optional[Any] = None
    ) -> ResponseDict:
        "成功的返回值"
        return {
            'status': 'ok',
            'code': 1000,
            'message': 'Success',
            'data': data
        }
    
    @staticmethod
    def get_error_response(
        code: str,
        message: str,
        error_id: str
    ) -> ResponseDict:
        "失败的返回值"
        return {
            'status': 'error',
            'code': code,
            'message': message,
            'data': {
                'error_id': error_id
            }
        }
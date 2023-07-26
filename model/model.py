from pydantic import BaseModel
from typing import Union, Optional


class Data(BaseModel):
    building_id: str
    sign: str
    func: str
    data: str
    datetime: str
    value_type: str
    data_type: str


class UploadData(BaseModel):
    data: Union[Data, list] = None
    building_id: str
    frequency: int
    value_type: int
    data_type: int
    table_name: str = None


class User(BaseModel):
    username: str
    password: str


class DataSend(BaseModel):
    building_id: str
    frequency: int
    value_type: int
    data_type: int
    start_time: str
    end_time: str
    sign_list: list = []
    func_list: list = []
    flag: int
    table_name_get: str
    history: str = None


class DefaultResponse(BaseModel):
    code: int = 200
    msg: str = "success"


class ResponseData(DefaultResponse):
    data: Union[Data, list] = None


class LoginResponse(DefaultResponse):
    user_id: str = None


class Rules(BaseModel):
    range: list = []
    null: bool = False
    zero: bool = False
    float: int
    negative: bool = False
    growth: bool = False


class FakeData(BaseModel):
    building_id: str
    frequency: int
    value_type: int
    data_type: int
    start_time: str
    end_time: str = None
    sign_list: list = []
    func_list: list = []
    table_name_save: str
    size: int
    rules: Union[Rules, dict] = None
    coiled: bool = True

from pydantic import BaseModel
from datetime import datetime

class BuildingInfo(BaseModel):
    code:str
    name:str=None
    area:float=0
    population:str=None
    project_name:str=None
    project_code:str=None
    province:str=None
    city:str=None
    district:str=None
    project_function_type:str=None
    region_code:str=None
    domain_name:str=None

    class Config:
        orm_mode = True

class DataCommon(BaseModel):
    total_energy:float=0
    total_quota:float=0
    total_guiding:float=0
    time:datetime=None
    building_code:str

    class Config:
        orm_mode = True

class TotalData(DataCommon):
    total_clean:float=0

class DataByType(DataCommon):
    type:str

class DataSub(DataCommon):
    sub_name:str


class PlatformConfig(BaseModel):
    platform_id:int
    name:str
    inside_ip:str
    inside_port:int
    outside_ip:str
    outside_port:int
    domain_name:str
    created_time:datetime
    destination:str=None
    receive_data_or_not:bool=None
    class Config:
        orm_mode=True

class SetDataReceiveRequest(BaseModel):
    status:bool
    platform_id:int


class TableWithData(BaseModel):
    table_name:str
    data:list[dict]


class NoneEnergyRequest(BaseModel):
    target_platform_id:str
    data_list:list[TableWithData]


class EnergyRequest(BaseModel):
    project_id:str
    building_id:str
    platform_id:str
    data_list:list[TableWithData]
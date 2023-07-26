from fastapi import FastAPI, Depends, HTTPException, UploadFile,File,Form
from sqlalchemy.orm import Session
import logging
import asyncio

from api import models, crud, schemas,log,exceptions
from api.databases import engine, SessionLocal
from api.utils import control_web_service,gather_in_bulk
import settings
from pprint import pformat
from pathlib import Path
import zipfile

logger = log.logger

models.Base.metadata.create_all(bind=engine)
app = FastAPI(debug=log.is_debug)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post('/building_info/bulk_create')
def bulk_create_building_info(building_info_list: list[schemas.BuildingInfo], db: Session = Depends(get_db)):
    crud.bulk_create_building_info(db, building_info_list)


@app.post('/data_total/bulk_create/{date_type}')
def bulk_create_energy_cost_total_data(data_list: list[schemas.TotalData], date_type: str, db: Session = Depends(get_db)):
    if date_type not in ['h', 'd', 'm']:
        return HTTPException(status_code=401, detail=f'非法的date_type')
    crud.bulk_create_energy_cost_total(db, data_list, date_type)


@app.post('/data_by_type/bulk_create/{date_type}')
def bulk_create_energy_cost_by_type_data(data_list: list[schemas.DataByType], date_type: str, db: Session = Depends(get_db)):
    if date_type not in ['h', 'd', 'm']:
        return HTTPException(status_code=401, detail=f'非法的date_type')
    crud.bulk_create_energy_cost_by_type(db, data_list, date_type)


@app.post('/data_sub/bulk_create/{date_type}')
def bulk_create_energy_cost_sub_data(data_list: list[schemas.DataSub], date_type: str, db: Session = Depends(get_db)):
    if date_type not in ['h', 'd', 'm']:
        return HTTPException(status_code=401, detail=f'非法的date_type')
    crud.bulk_create_energy_cost_sub(db, data_list, date_type)

# from typing import List
# @app.get('/platform_config_list',response_model=List[schemas.PlatformConfig])
# def get_platform_config_list(db: Session = Depends(get_db)):
#     data = crud.get_platform_config_list(db)
#     return data

# @app.post('/platform_config/set_receive_data')
# def set_receive_data(data:schemas.SetDataReceiveRequest,db: Session = Depends(get_db)):
#     crud.set_receive_data(db,data.platform_id,data.status)


@app.post('/sync_db/energy')
async def sync_energy(request_data:schemas.EnergyRequest):
    project_id = request_data.project_id
    platform_id = request_data.platform_id
    building_id = request_data.building_id
    data_list = request_data.data_list

    tasks = []
    for table_with_data in data_list:
        tasks.append(crud.sync_energy_or_meter_table(project_id,platform_id,table_with_data))
    
    if len(tasks)>0:
        try:
            # await asyncio.gather(*tasks)
            await gather_in_bulk(tasks)
        except exceptions.CustomException as err:
            logger.error(f'sync_energy_or_meter_table err:{err.args[0]}')
            return HTTPException(status_code=1001, detail={'error':f'sync_energy_or_meter_table err:{err.args[0]}'})


@app.post('/sync_db/non_energy')
async def sync_non_energy(request_data:schemas.NoneEnergyRequest,db: Session = Depends(get_db)):
    """：
    1. 接收数据
    2. 对于每一张表，插入临时表
    3. 停止web服务
    4. 对于每一张表，修改正式和临时表名，对于project_config表，要根据变化创建或者删除数据库
    5. 启动服务
    """
    logger.info('sync non_energy tables')
    tasks = []
    table_names = []
    data_list = request_data.data_list
    target_platform_id = request_data.target_platform_id
    for data in data_list:
        table_name = data.table_name
        table_data_list = data.data
        if len(table_data_list)>0:
            table_names.append(table_name)
        tasks.append(crud.sync_table(table_name,table_data_list,target_platform_id,db))
    if len(tasks)>0:        
        try:
            result_list = await asyncio.gather(*tasks)
        except exceptions.CustomException as err:
            logger.error(f'sync_table err:{err.args[0]}')
            return HTTPException(status_code=1001, detail={'error':f'sync_table err:{err.args[0]}'})
    else:
        return

    # 所有表的数据都为空
    if len(table_names)==0:
        return

    logger.info('stop web service')
    control_web_service('stop')
    logger.info('web service stopped')
    # 创建或删除能耗数据库
    db_tasks = []
    for result in result_list:
        if result is not None:
            projects_to_add = result['projects_to_add']
            logger.debug(f'create databases for {pformat(projects_to_add)}')
            for db_config in projects_to_add:
                db_tasks.append(crud.create_or_drop_db(db_config,'create'))

            projects_to_del = result['projects_to_del']
            logger.debug(f'delete databases for {pformat(projects_to_del)}')
            for db_config in projects_to_del:
                db_tasks.append(crud.create_or_drop_db(db_config,'drop'))
    if len(db_tasks)>0:
        try:
            await asyncio.gather(*db_tasks)
        except exceptions.CustomException as err:
            logger.error(f'create or delete db err:{err.args[0]}')
            logger.info('start web service')
            control_web_service('start')
            logger.info('web service started') 
            return HTTPException(status_code=1001, detail={'error':f'create or delete db err:{err.args[0]}'})

    logger.info('rename tables')
    rename_tasks = []
    for table_name in table_names:
        rename_tasks.append(crud.rename_table(table_name,db))

    if len(rename_tasks)>0:
        try:
            await asyncio.gather(*rename_tasks)
        except exceptions.CustomException as err:
            logger.error(f'rename tables err:{err.args[0]}')
            logger.info('start web service')
            control_web_service('start')
            logger.info('web service started')
            return HTTPException(status_code=1001, detail={'error':f'rename tables err:{err.args[0]}'})
       
    logger.info('start web service')
    control_web_service('start')
    logger.info('web service started')

    
# @app.post('/upload_attachment_files')
# async def upload_attachment_files(file:UploadFile=File(...)):
#     """
#     1. 接收上传的压缩文件
#     2. 解压缩到指定的目录
#     重复上传，覆盖同名文件
#     """
#     dest_dir="/home/bnse/workspace/new_energy_server/compose/files"
#     path = Path(dest_dir)
#     logger.debug(f'create dir {dest_dir}')
#     path.mkdir(parents=True, exist_ok=True)
#     try:
#         with zipfile.ZipFile(file.file._file) as zf:
#             for tmp_file in zf.filelist:
#                 extracted_path = Path(zf.extract(tmp_file,path=path))
#                 re_code_filename = tmp_file.filename.encode('cp437').decode('gbk')
#                 full_path = path.joinpath(re_code_filename)
#                 extracted_path.rename(full_path)
#     except Exception as err:
#         logger.error(f'unzip file err:{err.args[0]}')
#         return HTTPException(status_code=1001, detail={'error':f'unzip file err:{err.args[0]}'})


@app.post('/sync_db/non_energy2')
async def sync_non_energy2(request_data:schemas.NoneEnergyRequest,db: Session = Depends(get_db)):
    """
    1. 关闭web
    2. 替换数据
    3. 启动web
    """
    platform_id = request_data.target_platform_id
    table_data_list = request_data.data_list
    total_data_amount = 0
    for table_data in table_data_list:
        data = table_data.data
        total_data_amount += len(data)
    if total_data_amount == 0:
        logger.info('no data, return')
        return

    # logger.info('stop web service')
    # control_web_service('stop')
    # logger.info('web service stopped')

    replace_data_tasks = []
    for table_data in table_data_list:
        table_name = table_data.table_name
        data = table_data.data
        replace_data_tasks.append(crud.replace_table_data(table_name,data,platform_id,db))

    try:
        await asyncio.gather(*replace_data_tasks)
    except Exception as err:
        logger.error(f'replace table data err:{err.args[0]}')
        logger.info('start web service')
        control_web_service('start')
        logger.info('web service started')
        return HTTPException(status_code=1001, detail={'error':f'replace table data err:{err.args[0]}'})
       
    # logger.info('start web service')
    # control_web_service('start')
    # logger.info('web service started')
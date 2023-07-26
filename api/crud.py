from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from api.log import logger

from pprint import pformat
from sqlalchemy import Table, UniqueConstraint,text
from sqlalchemy.engine.base import Connection
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists,create_database,drop_database
from collections import namedtuple
from api.databases import engine_config
import asyncio
from sqlalchemy import MetaData


from api import models, schemas,utils,exceptions

def bulk_create_building_info(db:Session, building_info_list:list[schemas.BuildingInfo]):
    """
    创建或者根据主键更新建筑数据
    """
    logger.info(f'向表<{models.BuildingInfo.__tablename__}>插入数据')
    logger.debug(pformat(building_info_list))
    for bi in building_info_list:
        obj = models.BuildingInfo(
            code = bi.code,
            name = bi.name,
            area = bi.area,
            population = bi.population,
            project_name = bi.project_name,
            project_code = bi.project_code,
            province = bi.province,
            city = bi.city,
            district = bi.district,
            project_function_type = bi.project_function_type,
            region_code = bi.region_code,
            domain_name = bi.domain_name
        )
        db.merge(obj)
    db.commit()


def bulk_create_energy_cost_total(db:Session, data_list:list[schemas.TotalData], date_type:str):
    table = getattr(models, f'BuldingEnergyCostTotal{date_type.upper()}')
    bulk_create_or_update_data(db, data_list, table)


def bulk_create_energy_cost_by_type(db:Session, data_list:list[schemas.DataByType], date_type:str):
    table = getattr(models, f'BuldingEnergyCostByType{date_type.upper()}')
    bulk_create_or_update_data(db, data_list, table)


def bulk_create_energy_cost_sub(db:Session, data_list:list[schemas.DataSub], date_type:str):
    table = getattr(models, f'BuldingEnergyCostSub{date_type.upper()}')
    bulk_create_or_update_data(db, data_list, table)


def bulk_create_or_update_data(db:Session, data_list:list[schemas.BaseModel],table:models.Base):
    """
    使用insert on duplicated updated插入或者更新数据(批量会产生死锁，使用逐条提交)
    有外键约束错误的数据不会插入，其他数据会插入
    更新的数据去掉唯一索引字段
    """
    constraints = table.__table__.constraints
    unique_constraint = [o for o in constraints if isinstance(o,UniqueConstraint)][0]
    unique_constraint_columns = unique_constraint.columns.keys()
    logger.info(f'向表<{table.__tablename__}>插入数据')
    logger.debug(pformat(data_list))
    for data in data_list:
        data_dict = data.dict()
        update_dict = {k:v for k,v in data_dict.items() if k not in unique_constraint_columns}
        insert_stmt = insert(table).values(**data_dict).on_duplicate_key_update(**update_dict)
        try:
            db.execute(insert_stmt)
        except IntegrityError as err:
            logger.error(err)
        db.commit()
    

# def bulk_create_or_update_data(db:Session, data_list:list[schemas.BaseModel],table:models.Base):
#     """
#     逐条数据判断，如果不存在就插入，存在就更新
#     """
#     logger.info(f'向表<{table.__tablename__}>插入数据')
#     logger.debug(pformat(data_list))


# def get_platform_config_list(db:Session):
#     return db.query(PlatformConfig).all()

# def set_receive_data(db:Session,platform_id:int,status:bool):
#     obj=db.query(PlatformConfig).get(platform_id)
#     if obj is None:
#         raise HTTPException(status_code=404,detail='platform_config not found')
    
#     obj.receive_data_or_not = status
#     db.commit()
#     return obj


# def get_receive_data(db:Session):
#     obj=db.query(PlatformConfig).get(platform_id)
#     if obj is None:
#         logger.error(f'平台id<{platform_id}>在PlatformConfig中不存在')
#         return False
#     return bool(obj.receive_data_or_not)


async def sync_table(table_name:str,table_data_list:list,platform_id:str,db: Session)->str:
    """
    1. 创建临时表
    2. 插入数据
    """
    if len(table_data_list)==0:
        logger.info(f'no data for table <{table_name}> to sync, skip')
        return
    else:
        logger.info(f'sync table: {table_name}')
    table = await get_table_from_name(table_name,db)
    engine = table.bind
    with engine.connect() as conn:
        if table_name == 'r_user':
            await update_r_user_data(table_data_list,conn)
        await create_tmp_table(table)
        await insert_data_into_table(conn,table_data_list,table)
        if table_name == 'project_config':
            project_diff = await get_project_diff(conn,table_data_list,platform_id)
    engine.dispose()
    return project_diff


async def update_r_user_data(table_data_list:list,conn:Connection):
    """
    更新table_data_list,系统用户的密码那条数据使用当前数据库中的，而非传入的
    c_object_code == 61,c_code==200005
    """
    sql_obj = 'select * from r_user where c_object_code=61 and c_code=200005'
    
    result_proxy = conn.execute(sql_obj)
    result = result_proxy.fetchall()
    map_user_id2password = {}
    for data in result:
        map_user_id2password[data[1]]=data[1]

    for data in table_data_list:
        if str(data['c_object_code']) == '61' and str(data['c_code']) == '200005':
            password_in_db = map_user_id2password.get(data['c_local_id'])
            if password_in_db is not None:
                data['c_value'] = password_in_db


async def create_tmp_table(table:Table):
    """
    创建一张临时表(如果存在先删除)，表名为原表名后缀_tmp
    """
    table_name = table.name
    tmp_table_name = f'{table_name}_tmp'
    table.name = tmp_table_name
    if table.exists():
        logger.info(f'删除表{tmp_table_name}')
        table.drop()
    
    logger.info(f'创建临时表{tmp_table_name}')
    table.create()


async def insert_data_into_table(conn:Connection,data_list:list,table:Table):
    """
    向表中插入数据
    """
    table = Table(table.name,MetaData(bind=table.bind),autoload=True,autoload_with=table.bind)
    logger.info(f'向表{table.name}中插入数据')
    
    insert_obj = table.insert()
    
    logger.info(insert_obj)
    start = 0
    step = 100
    total = len(data_list)
    while start<=total:
        stop = start+step
        conn.execute(insert_obj,data_list[start:stop])
        start = stop
    # conn.execute(insert_obj,data_list)


async def rename_table(table_name:str,db: Session):
    """
    1. 把原表名修改为 table_name_bak_timestamp
    2. 把临时表table_name_tmp修改为 table_name
    """
    table = await get_table_from_name(table_name)
    engine = table.bind
    tmp_table_name = f'{table_name}_tmp'
    bak_table_name = f'{table_name}_bak_{utils.get_now_suffix()}'
    rename_old_sql = f'rename table {table_name} to {bak_table_name};'
    rename_tmp_sql = f'rename table {tmp_table_name} to {table_name};'
    roll_back_bak_sql = f'rename table {bak_table_name} to {table_name}'

    try:
        with engine.connect() as conn:

            logger.debug(rename_old_sql)
            conn.execute(rename_old_sql)
            try:
                conn.execute(f'desc {bak_table_name}')
            except Exception as err:
                logger.error(f'rename old table err:{err.args[0]}')
                raise exceptions.CustomException(f'{rename_old_sql} failed')
            
            logger.debug(rename_tmp_sql)
            conn.execute(rename_tmp_sql)
            try:
                conn.execute(f'desc {table_name}')
            except Exception as err:
                logger.error(f'rename tmp table err:{err.args[0]},roll back')
                conn.execute(roll_back_bak_sql)
                raise exceptions.CustomException(f'{rename_tmp_sql} failed')
    except Exception as err:
        raise err
    finally:
        engine.dispose()


async def get_table_from_name(table_name:str,project_id:str=None)->Table:
    """
    通过project_config中的数据生成所有数据库的engines，再生成Table
    """
    config_table = Table('project_config',MetaData(bind=engine_config),autoload=True,autoload_with=engine_config)
    with engine_config.connect() as conn:
        db_config_list = conn.execute(config_table.select()).fetchall()
    
    engines = [engine_config]
    for config in db_config_list:
        tmp_project_id = config[2]
        database_name = config[5]
        database_type = config[6]
        database_ip = config[9]
        database_user = config[7]
        database_passwd = config[8]
        database_port = config[10]

        db_name = f'{tmp_project_id}_{database_name}' if tmp_project_id else database_name
        host_ip = 'new_energy_server_db_1' if database_type!='全局' else database_ip
        url_str = f"mysql+pymysql://{database_user}:{database_passwd}@{host_ip}:{database_port}/{db_name}"
        tmp_engine = create_engine(url_str)
        engine_var_name = f'engine_{db_name}'
        locals()[engine_var_name] = tmp_engine
        engines.append(tmp_engine)
    
    engine_config_var_name = engine_config
    engine_user_var_name = locals()['engine_bnse_user']
    engine_cloud_var_name = locals()['engine_bnse_cloud']

    # 非energy固定，否则查询建筑和项目的关系
    # 添加新的项目的话，需要先传非能耗数据，再传递能耗数据
    map_non_energy_table2db_name = {
        't_dictionary_func_no':engine_config_var_name,
        't_dictionary_entity_code':engine_config_var_name,
        'project_config':engine_config_var_name,
        'platform_config':engine_config_var_name,
        'operation_log':engine_config_var_name,
        'area_coverage':engine_config_var_name,

        'r_user':engine_user_var_name,
        'u_relation':engine_user_var_name,

        'r_attachment':engine_cloud_var_name,
        'r_building':engine_cloud_var_name,
        'r_equipment_mapping':engine_cloud_var_name,
        'r_equipment':engine_cloud_var_name,
        'r_custom_func':engine_cloud_var_name,
        'r_relation':engine_cloud_var_name,
        'r_set_quota':engine_cloud_var_name,
        'r_virtual':engine_cloud_var_name,
    }
    
    engine = map_non_energy_table2db_name.get(table_name)
    if engine is None:
        map_bd_id2project_id = {}
        
        with engine_cloud_var_name.connect() as conn:
            sql = "select * from r_building where c_object_code=12 and c_code=100004;"
            result = conn.execute(sql)
            result_data = result.fetchall()
            for data in result_data:
                map_bd_id2project_id[data[1]]=data[4]
        engine_cloud_var_name.dispose()
        
        energy_table_key_str2db = {
            'accumulate':'meter',
            'equipment':'meter',
            'recentdatas':'meter',
            'buildingcomputetime':'meter',
            'buildinguploadtime':'meter',
            'building_equipment_compute_last_data_':'meter',
            'running':'energy'
        }
                        

        for key_str,db_str in energy_table_key_str2db.items():
            if key_str in table_name:
                engine_name_var_str = f'engine_{project_id}_{db_str}'
                tmp_engine = locals().get(engine_name_var_str)
                if tmp_engine is not None:
                    engine = tmp_engine
                    break
    
    if engine is not None:
        logger.info(f'获取表{engine}.{table_name}')
        return Table(table_name,MetaData(bind=engine))
    
    # for tmp_engine in engines:
    #     try:
    #         if tmp_engine.has_table(table_name):
    #             return Table(table_name,MetaData(bind=tmp_engine),autoload=True,autoload_with=tmp_engine)
    #     except Exception:
    #         # logger.warning(f'创建连接<{url_str}>失败')
    #         pass

    raise exceptions.CustomException(f'{table_name}无法找到所在数据库')
  

async def get_project_diff(conn:Connection,data_list:list[dict],platform_id:str)->dict[str,set]:
    """
    获取project_config表中新添加的项目id和被删除的项目id
    """
    table = await get_table_from_name('project_config')
    table = Table(table.name,MetaData(bind=table.bind),autoload=True,autoload_with=table.bind)
    result_obj = conn.execute(table.select().filter_by(platform_id=platform_id))
    old_data = result_obj.fetchall()
    old_project_id_set = set(o[2] for o in old_data if o[2] is not None)
    new_project_id_set = set(o['project_id'] for o in data_list if o['project_id'] is not None)

    project_ids_to_add=new_project_id_set-old_project_id_set
    projects_to_add = []
    for data in data_list:
        if data['project_id'] in project_ids_to_add:
            projects_to_add.append({
                'project_id':data['project_id'],
                'database_user':data['database_user'],
                'database_passwd':data['database_passwd'],
                'database_port':data['database_port'],
                'database_name':data['database_name'],
                'database_type':data['database_type'],
                'database_ip':data['database_ip'],
            })

    project_ids_to_del=old_project_id_set-new_project_id_set
    projects_to_del = []
    for data in old_data:
        if data[2] in project_ids_to_del:
            projects_to_del.append({
                'project_id':data[2],
                'database_user':data[7],
                'database_passwd':data[8],
                'database_port':data[10],
                'database_name':data[5],
                'database_type':data[6],
                'database_ip':data[9],
            })
    
    return {
        'projects_to_add':projects_to_add,
        'projects_to_del':projects_to_del
    }


async def create_or_drop_db(db_config:dict,action:str):
    """
    创建/删除一个数据库
    """
    if action not in ['create','drop']:
        return
    logger.info(f'{action} database {db_config}')
    db_name = f'{db_config["project_id"]}_{db_config["database_name"]}' if db_config["project_id"] else db_config["database_name"]
    username = db_config['database_user']
    password = db_config['database_passwd']
    port = db_config['database_port']
    host_ip = 'new_energy_server_db_1' if db_config['database_type']!='全局' else db_config['database_ip']
    url_str = f"mysql+pymysql://{username}:{password}@{host_ip}:{port}/{db_name}"
    engine=create_engine(url_str)
    url_obj = engine.url
    if action == 'create':
        if not database_exists(url_obj):
            logger.info(f'创建数据库{url_str}')
            create_database(url_obj)
    else:
        if database_exists(url_obj):
            logger.info(f'删除数据库{url_str}')
            drop_database(url_obj)
    return engine


async def delete_data_from_table(conn:Connection,table:Table,from_time:str,to_time:str):
    """
    删除表中时间范围内的数据，对于recent表删除所有数据
    """
    logger.info(f'删除数据,[{from_time},{to_time}],<table_name:{table.name}>')
    if  'recent' in table.name:
        delete_sql_obj = table.delete()
    else:
        delete_sql_obj = table.delete(text(f"c_receivetime>='{from_time}' and c_receivetime<='{to_time}'"))
    logger.debug(delete_sql_obj)
    conn.execute(delete_sql_obj)


async def sync_energy_or_meter_table(project_id:str,platform_id:str,table_with_data:dict):
    """
    对于meter表和energy表，删除旧数据并插入新数据，要删除的旧数据根据新数据的时间范围来确定
    """
    table_name = table_with_data.table_name
    data_list = table_with_data.data
    if len(data_list)==0:
        logger.info(f'no data for table <{table_name}> to sync, skip')
        return
    else:
        logger.info(f'sync table: {table_name}')

    # 如果表存在
    try:
        table = await get_table_from_name(table_name,project_id=project_id)
        table_exists = table.exists()
    except Exception as err:
        logger.warning(f'{err.__class__}:{err}')
        table_exists = False
    
    if not table_exists:
        # 如果表不存在
        create_table_sql = f'''
            CREATE TABLE if not exists `{table_name}` (
                `c_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                `c_local_id` varchar(50) NOT NULL,
                `c_func` varchar(30) NOT NULL,
                `c_value` varchar(30) NOT NULL,
                `c_receivetime` datetime NOT NULL,
                `c_type` varchar(10) DEFAULT NULL,
                `c_remark` varchar(50) DEFAULT NULL,
                PRIMARY KEY (`c_id`) USING BTREE,
                KEY `index_name` (`c_local_id`,`c_func`,`c_receivetime`) USING BTREE
            ) ENGINE=InnoDB AUTO_INCREMENT=146163 DEFAULT CHARSET=utf8;
        '''
        
        if 'running' in table_name:
            database_name = 'energy'
        else:
            database_name = 'meter'
        config_table = Table('project_config',MetaData(bind=engine_config),autoload=True,autoload_with=engine_config)
        with engine_config.connect() as conn:
            select_obj = config_table.select()
            if platform_id:
                select_obj = select_obj.filter_by(platform_id=platform_id)
            if project_id:
                select_obj = select_obj.filter_by(project_id=project_id)
            db_config_list = conn.execute(select_obj).fetchall()
       
        if len(db_config_list) ==0:
            raise exceptions.CustomException(f'无法获得<project_id:{project_id},platform_id:{platform_id},database_name:{database_name}>数据库配置')
        config_obj = db_config_list[0]
        config = {
            'project_id':config_obj[2],
            'database_name':config_obj[5],
            'database_user':config_obj[7],
            'database_passwd':config_obj[8],
            'database_port':config_obj[10],
            'database_type':config_obj[6],
            'database_ip':config_obj[9]
        }

        engine = await create_or_drop_db(config,'create')
        metadata = MetaData(bind=engine)
        logger.info(f'创建表<{table_name}>')
        try:
            with engine.connect() as conn:
                conn.execute(create_table_sql)
        finally:
            engine.dispose()
        table = Table(table_name,metadata,autoload=True,autoload_with=engine)
        logger.debug(f'创建连接<{engine.url}>的新表<{table_name}>')

    engine = table.bind
    try:
        with engine.connect() as conn:
            from_time = data_list[0]['c_receivetime']
            to_time = data_list[-1]['c_receivetime']
            await delete_data_from_table(conn,table,from_time,to_time)
            await insert_data_into_table(conn,data_list,table)
    finally:
        engine.dispose()


async def replace_table_data(table_name:str,data:list[dict],platform_id:str):
    """
    在一个事务中
        1. 删除旧数据
        2. 插入新数据
        3. 对于project_config表，要根据变化创建或者删除数据库
    """
    table = await get_table_from_name(table_name)
    engine = table.bind
    table = Table(table.name,MetaData(bind=table.bind),autoload=True,autoload_with=table.bind)
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(table.delete())

            insert_obj = table.insert()
    
            logger.info(insert_obj)
            start = 0
            step = 1
            total = len(data)
            while start<total:
                stop = start+step
                conn.execute(insert_obj,data[start:stop])
                start = stop
    engine.dispose()
    
    if table_name == 'project_config':
        proj_diff = await get_project_diff(conn,data,platform_id)
        projects_to_add = proj_diff['projects_to_add']
        projects_to_del = proj_diff['projects_to_del']
        db_tasks = []
        for db_config in projects_to_add:
            db_tasks.append(create_or_drop_db(db_config,'create'))
        for db_config in projects_to_del:
            db_tasks.append(create_or_drop_db(db_config,'drop'))

        await asyncio.gather(*db_tasks)
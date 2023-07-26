import asyncio
import datetime
from pathlib import Path
import logging
import logging.config
import json
import pandas as pd
from sqlalchemy import create_engine

handler_dict_debug = {
    'level': 'DEBUG',
    'class': 'logging.StreamHandler',
    'formatter': 'standard'
}
log_config = {
    'version': 1,
    'formatters': {
        'standard': {  # 简单格式
            'format': '[%(asctime)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] '
                      '[%(levelname)s]- %(message)s'
        }
    },
    'handlers': {},
    'loggers': {}
}


class WarningFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno == logging.WARNING


def gen_handlers_dict(logs_dir: Path, prefix: str):
    return {
        f'{prefix}_info': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': logs_dir.joinpath(f'{prefix}_info.log'),
            'when': 'midnight',
            'backupCount': 10,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码
        },
        f'{prefix}_debug': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': logs_dir.joinpath(f'{prefix}_debug.log'),
            'when': 'midnight',
            'backupCount': 10,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码
        },
        f'{prefix}_warning': {
            'level': 'WARNING',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': logs_dir.joinpath(f'{prefix}_warning.log'),
            'filters': [WarningFilter()],
            'when': 'midnight',
            'backupCount': 10,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码
        },
        f'{prefix}_error': {
            'level': 'ERROR',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': logs_dir.joinpath(f'{prefix}_error.log'),
            'when': 'midnight',
            'backupCount': 10,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码
        },
        'debug': handler_dict_debug
    }


def gen_logger_dict(logger_name: str, handler_list: list[str]):
    return {
        logger_name: {
            'handlers': handler_list,
            'level': 'INFO',
            'propagate': True
        }
    }


def get_logger(prefix: str = '', log_type: str = 'UploadData') -> logging.Logger:
    """
        添加日志，并返回
        bd_id: 日志前缀，例如main, bd_id
        log_type: 日志类型，相同类型的日志保存在同一个目录下
    """
    logs_dir = Path().joinpath(f'{log_type}_logs')
    logs_dir.mkdir(exist_ok=True)

    handlers_dict = gen_handlers_dict(logs_dir, prefix)
    log_config['handlers'].update(handlers_dict)
    logger_dict = gen_logger_dict(prefix, list(handlers_dict.keys()))
    log_config['loggers'].update(logger_dict)

    logging.config.dictConfig(log_config)
    return logging.getLogger(prefix)

logger = get_logger(prefix='IBMS', log_type='DataConversion')


# nest_asyncio.apply()

def write_to_db(df, write_table_name, batch_size, engine, read_table_name):
    stat = 0
    end = batch_size
    # columns = ['id', 'building_id', 'sign', 'func', 'datetime', 'value_type', 'data_type', 'data']
    columns = ['sign', 'func', 'data', 'datetime', 'building_id', 'value_type', 'data_type']
    title = read_table_name + ":" + str(datetime.datetime.now())
    # pbar = tqdm(total=len(df), desc=title)
    # with tqdm(total=len(df), desc=title) as pbar:
    while True:
        batch_df = df.iloc[stat:end]
        if batch_df.empty:
            break
            # batch_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        data_list = batch_df.astype(str).values.tolist()
        data_str = ','.join([str(tuple(i)) if isinstance(i, list) else str(i) for i in data_list])
        insert_sql = f"""insert into `{write_table_name}` ({",".join(columns)}) values {data_str} on duplicate key update {",".join([f'{i} = values({i})' for i in columns])}"""
        engine.execute(insert_sql)
        engine.dispose()
        stat += batch_size
        end += batch_size
        # pbar.update(len(batch_df))


# 对数据处理
def data_processing(df, data_type, building_id, value_type):
    df.drop(columns=['c_id', 'c_type', 'c_remark'], inplace=True)
    df['building_id'] = building_id
    df['value_type'] = value_type
    df_new = df.rename(columns={'c_local_id': 'sign', 'c_func': 'func', 'c_value': 'data',
                                'c_receivetime': 'datetime'})
    df_new['data_type'] = data_type
    return df_new


async def write_db():
    with open('config_data.json', 'r', encoding='utf-8') as f:
        config_data = json.load(f)
        f.close()
    start_time = config_data.get('start_time')
    config_database = config_data.get('config_database')
    BASE_SQLALCHEMY_DATABASE_URL = f'mysql+pymysql://{config_database.get("username")}:' \
                                   f'{config_database.get("password")}@{config_database.get("host")}:' \
                                   f'{config_database.get("port")}/{config_database.get("database")}?charset=utf8'
    engine = create_engine(BASE_SQLALCHEMY_DATABASE_URL)
    sql = "select * from project_config where database_type='后端' and database_name in ('energy','meter')"
    df_base = pd.read_sql(sql, engine)
    database_list = df_base.to_dict(orient='records')
    store_database = config_data.get('store_database')
    STORE_SQLALCHEMY_DATABASE_URL = f'mysql+pymysql://{store_database.get("username")}:' \
                                   f'{store_database.get("password")}@{store_database.get("host")}:' \
                                   f'{store_database.get("port")}/{store_database.get("database")}?charset=utf8'
    write_engine = create_engine(STORE_SQLALCHEMY_DATABASE_URL)

    tasks = []

    for database in database_list:
        database_name = database.get("project_id") + '_' + database.get("database_name")
        READ_SQLALCHEMY_DATABASE_URL = f'mysql+pymysql://{database.get("database_user")}:' \
                                           f'{database.get("database_passwd")}@{database.get("database_ip")}:' \
                                           f'{database.get("database_port")}/{database_name}?charset=utf8mb4'
        try:
            read_engine = create_engine(READ_SQLALCHEMY_DATABASE_URL)
            with read_engine.connect() as read_conn:
                result = read_conn.execute(
                        f'SELECT table_name FROM information_schema.tables WHERE table_schema="{database_name}"'
                    )
        except Exception as e:
            logger.error(e)
            continue
        table_names = [row[0] for row in result]
        recentdatas_list = []
        for table_name in table_names:
            name_list = table_name.split("_")
            if len(name_list) == 2 and name_list[0] == 'recentdatas':
                recentdatas_list.append(table_name)
                continue
            elif len(start_time) == 0:
                if len(name_list) == 4 and name_list[1] == 'running':
                    df = pd.read_sql_table(table_name=table_name, con=read_engine)
                    if name_list[-1] == 'm1':
                        data_type = 1
                        freq = 60 * 60 * 24 * 30
                    elif name_list[-1] == 'd1':
                        data_type = 2
                        freq = 60 * 60 * 24
                    elif name_list[-1] == 'h1':
                        data_type = 3
                        freq = 60 * 60
                    elif name_list[-1] == 'm10':
                        data_type = 4
                        freq = 60 * 10
                    else:
                        logger.error(f"数据类型错误")
                        # raise Exception("data_type ERROR")
                    df_new = data_processing(df=df, data_type=data_type, building_id=name_list[2], value_type=2)
                    last_time = df_new['datetime'].max()
                elif len(name_list) == 4 and name_list[1] == 'equipment':
                    df = pd.read_sql_table(table_name=table_name, con=read_engine)
                    freq = 60 * 10
                    df_new = data_processing(df=df, data_type=4, building_id=name_list[2], value_type=2)
                    last_time = df_new['datetime'].max()
                else:
                    continue
                write_to_db(df=df_new, write_table_name='completion_data', batch_size=5000, engine=write_engine,
                            read_table_name=table_name)
                logger.info(f"{table_name}已经写入新表{datetime.datetime.now()}")
                # print(f"{table_name}已经写入新表{datetime.datetime.now()}")
                task = asyncio.ensure_future(
                    send_data(table_name=table_name, freq=freq, read_engine=read_engine, write_engine=write_engine,
                              end_time=last_time))
                tasks.append(task)
            elif len(name_list) == 4:
                try:
                    time_list = start_time.split('-')
                    limit_time = time_list[0] + time_list[1]
                except:
                    raise Exception('start_time ERROR')
                if name_list[0] >= limit_time:
                    if name_list[1] == 'running':
                        df = pd.read_sql_table(table_name=table_name, con=read_engine)
                        if name_list[-1] == 'm1':
                            data_type = 1
                            freq = 60 * 60 * 24 * 30
                        elif name_list[-1] == 'd1':
                            data_type = 2
                            freq = 60 * 60 * 24
                        elif name_list[-1] == 'h1':
                            data_type = 3
                            freq = 60 * 60
                        elif name_list[-1] == 'm10':
                            data_type = 4
                            freq = 60 * 10
                        else:
                            raise Exception("data_type ERROR")
                        df_new = data_processing(df=df, data_type=data_type, building_id=name_list[2], value_type=2)
                        last_time = df_new['datetime'].max()
                    elif name_list[1] == 'equipment':
                        df = pd.read_sql_table(table_name=table_name, con=read_engine)
                        freq = 60 * 10
                        df_new = data_processing(df=df, data_type=4, building_id=name_list[2], value_type=2)
                        last_time = df_new['datetime'].max()
                    else:
                        continue
                    write_to_db(df=df_new, write_table_name='completion_data', batch_size=5000, engine=write_engine,
                                read_table_name=table_name)
                    logger.info(f"{table_name}已经写入新表{datetime.datetime.now()}")
                    # print(f"{table_name}已经写入新表{datetime.datetime.now()}")
                    task = asyncio.ensure_future(
                        send_data(table_name=table_name, freq=freq, read_engine=read_engine, write_engine=write_engine,
                                  end_time=last_time))
                    tasks.append(task)
                else:
                    continue
            else:
                continue
        for recentdatas in recentdatas_list:
            name_list = recentdatas.split("_")
            df = pd.read_sql_table(table_name=recentdatas, con=read_engine)
            freq = 60 * 10
            df_new = data_processing(df=df, data_type=5, building_id=name_list[1], value_type=2)
            last_time = df_new['datetime'].max()
            write_to_db(df=df_new, write_table_name='completion_data', batch_size=5000, engine=write_engine,
                        read_table_name=recentdatas)
            logger.info(f"{recentdatas}已经写入新表{datetime.datetime.now()}")
            # print(f"{recentdatas}已经写入新表{datetime.datetime.now()}")
            task = asyncio.ensure_future(
                send_data(table_name=recentdatas, freq=freq, read_engine=read_engine, write_engine=write_engine,
                          end_time=last_time))
            tasks.append(task)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))


async def send_data(table_name, freq, read_engine, write_engine, end_time):
    now_time = datetime.datetime.now()
    while True:
        start_time = end_time - datetime.timedelta(seconds=freq)
        if end_time < now_time:
            sql = f"select * from `{table_name}` where c_receivetime >='{start_time}'and c_receivetime<='{now_time}'"
        else:
            sql = f"select * from `{table_name}` where c_receivetime >='{start_time}'and c_receivetime<='{end_time}'"
        df_new = pd.read_sql(sql=sql, con=read_engine)
        # print(
        #     f'*********************************{table_name}******************************************************')
        if df_new.empty:
            await asyncio.sleep(freq)
            end_time = datetime.datetime.now()
            continue
        else:
            name_list = table_name.split("_")
            if len(name_list) == 4:
                if name_list[1] == 'running':
                    df_new = data_processing(df=df_new, data_type=4, building_id=name_list[2], value_type=2)
                elif name_list[1] == 'equipment':
                    df_new = data_processing(df=df_new, data_type=5, building_id=name_list[2], value_type=2)
                else:
                    continue
            else:
                df_new = data_processing(df=df_new, data_type=5, building_id=name_list[1], value_type=2)
            write_to_db(df=df_new, write_table_name='completion_data', batch_size=5000, engine=write_engine,
                        read_table_name=table_name)
            logger.info(f"{table_name}已经写入新表{datetime.datetime.now()}")
            await asyncio.sleep(freq)
            end_time = datetime.datetime.now()




if __name__ == '__main__':
    asyncio.run(write_db())

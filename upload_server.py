import calendar
import json
from datetime import datetime, timedelta
import random

import pandas as pd
from fastapi import FastAPI, Response

from api import log
from model.model import DefaultResponse, UploadData, User, LoginResponse, ResponseData, DataSend, FakeData
from settings import DATABASES
from utils.utils import decrypt_by_rsa, get_connect, check_password, generate_fake_data

logger = log.logger
app = FastAPI(debug=log.is_debug)
check_token_list = []


@app.middleware("http")
async def check_token(request, call_next):
    url = request.url.path
    if url in check_token_list:
        token = request.headers.get('Authorization')
        config_conn = get_connect(**DATABASES["config"])
        try:
            check_data = decrypt_by_rsa(token)
            check_data = json.loads(check_data)
        except Exception as e:
            logger.error(e)
            return Response(status_code=401, content="token error")
        finally:
            config_conn.dispose()
        cn = config_conn.execute(
            f"select * from `data_serve_auth` where building_id='{check_data['building_id']}' and user_id='{check_data['user_id']}'")
        if not cn.fetchone():
            return Response(status_code=401, content="token error")
        response = await call_next(request)
        return response
    else:
        response = await call_next(request)
        return response


@app.post('/DataReceive', response_model=DefaultResponse)
async def data_receive(request_data: UploadData):
    data = request_data.data
    frequency = request_data.frequency
    value_type = request_data.value_type
    data_type = request_data.data_type
    building_id = request_data.building_id
    table_name_save = request_data.table_name
    config_conn = get_connect(**DATABASES["config"])
    if data_type not in [0, 1, 2, 3, 4, 5]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if value_type not in [0, 1, 2, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if frequency not in [1, 2, 3, 4, 5]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if value_type == 0 and data_type == 2:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if data_type in [1, 4, 5] and value_type != 0:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    elif data_type == 2 and value_type not in [1, 2, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    elif data_type == 3 and value_type not in [0, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    database_sql = f"select * from `data_server_config` where building_id='{building_id}' and data_type='{data_type}'"
    match value_type:
        case 1:
            middlewares = "instant"
        case 2:
            middlewares = "accumulate"
        case 3:
            middlewares = "equipment"
    cn = config_conn.execute(database_sql)
    config_conn.dispose()
    database = cn.fetchone()
    if not database:
        return {"code": 1001, "msg": "授权校验失败"}
    data_database = {"username": database["username"], "password": database["password"], "host": database["host"],
                     "port": database["port"], "database": database["database"]}
    data_conn = get_connect(**data_database)
    pd_data = pd.DataFrame(data)
    pd_data["datetime"] = pd.to_datetime(pd_data["datetime"], format="ISO8601")
    start_time = pd_data["datetime"].min()
    # 根据sign分组，判断每组的时间间隔是否符合要求
    match frequency:
        case 1:
            for sign, group in pd_data.groupby("sign"):
                group = group.sort_values(by="datetime")
                # 判断datetime的时间结尾是不是1日0点0分0秒
                check_df = pd.DataFrame()
                check_df["check"] = (group["datetime"].dt.day == 1) & (group["datetime"].dt.hour == 0) & (group[
                                                                                                              "datetime"].dt.minute == 0) & (
                                            group["datetime"].dt.second == 0)
                if not check_df["check"].all():
                    return {"code": 1002, "msg": f"sign:{sign}数据频率不一致"}
            freq = "m1"
            # if data_type == "1" or data_type == "2":
            #     return {"code": 1004, "msg": "该类型数据不支持1小时频率"}
            if data_type != 3 and data_type != 4 and data_type != 0:
                return {"code": 1004, "msg": "该类型数据不支持1月频率"}
        case 2:
            for sign, group in pd_data.groupby("sign"):
                group = group.sort_values(by="datetime")
                # 判断datetime的时间结尾是不是0点0分0秒
                check_df = pd.DataFrame()
                check_df["check"] = (group["datetime"].dt.hour == 0) & (group["datetime"].dt.minute == 0) & (group[
                                                                                                                 "datetime"].dt.second == 0)
                if not check_df["check"].all():
                    return {"code": 1002, "msg": f"sign:{sign}数据频率不一致"}
            freq = "d1"
            # if data_type == "1" or data_type == "2":
            #     return {"code": 1004, "msg": "该类型数据不支持1小时频率"}
            if data_type != 3 and data_type != 4 and data_type != 0:
                return {"code": 1004, "msg": "该类型数据不支持1日频率"}
        case 3:
            for sign, group in pd_data.groupby("sign"):
                group = group.sort_values(by="datetime")
                # 判断datetime的时间结尾是不是0分0秒
                check_df = pd.DataFrame()
                check_df["check"] = (group["datetime"].dt.minute == 0) & (group["datetime"].dt.second == 0)
                if not check_df["check"].all():
                    return {"code": 1002, "msg": f"sign:{sign}数据频率不一致"}
            freq = "h1"
            # if data_type == "1" or data_type == "2":
            #     return {"code": 1004, "msg": "该类型数据不支持1小时频率"}
            if data_type != 3 and data_type != 4 and data_type != 0:
                return {"code": 1004, "msg": "该类型数据不支持1小时频率"}
        case 4:
            for sign, group in pd_data.groupby("sign"):
                group = group.sort_values(by="datetime")
                # 判断时间间隔是不是10分钟
                check_df = pd.DataFrame()
                check_df["check"] = group["datetime"].dt.minute % 10 == 0
                if not check_df["check"].all():
                    return {"code": 1002, "msg": f"sign:{sign}数据频率不一致"}
            freq = "m10"
            # if data_type == "1":
            #     return {"code": 1004, "msg": "该类型数据不支持10分钟频率"}
            if data_type != 2 and data_type != 3 and data_type != 0:
                return {"code": 1004, "msg": "该类型数据不支持10分钟频率"}
        case 5:
            freq = ""
            if data_type != 1 and data_type != 5 and data_type != 0:
                return {"code": 1004, "msg": "该类型数据不支持最近一条数据"}
        # case _:
        #     return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if data_type == 0:
        table_name = "completion_data"
        columns = ("building_id", "sign", "func", "data", "datetime", "value_type", "data_type")
        new_columns = ("building_id", "sign", "func", "data", "datetime", "value_type", "data_type")
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                            `id` int(256) NOT NULL AUTO_INCREMENT,
                            `building_id` varchar(64) NOT NULL,
                            `sign` varchar(64) NOT NULL,
                            `func` varchar(64) NOT NULL,
                            `datetime` datetime NOT NULL,
                            `value_type` int(2) NOT NULL,
                            `data_type` int(2) NOT NULL,
                            `data` varchar(255) NOT NULL,
                            PRIMARY KEY (`id`) USING BTREE,
                            UNIQUE KEY `building_id` (`building_id`,`sign`,`func`,`datetime`, `value_type`, `data_type`) USING BTREE,
                            KEY `building_id_2` (`building_id`,`sign`,`func`,`datetime`, `value_type`, `data_type`, `data`) USING BTREE
                        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;"""
    elif data_type == 1:
        if table_name_save:
            table_name = table_name_save
            columns = ("buildingid", "sign", "funcid", "data", "receivetime")
            new_columns = ("building_id", "sign", "func", "data", "datetime")
            create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                                `id` int(16) unsigned NOT NULL AUTO_INCREMENT,
                                `buildingid` varchar(20) NOT NULL,
                                `sign` varchar(20) NOT NULL,
                                `funcid` int(11) NOT NULL,
                                `data` double DEFAULT NULL,
                                `receivetime` datetime NOT NULL,
                                `flag` int(1),
                                PRIMARY KEY (`id`) USING BTREE,
                                KEY `index_intervaldata` (`buildingid`, `receivetime`) USING BTREE
                                ) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"""
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_recorddata_{database['building_id']}"
            columns = ("sign", "funcid", "data", "receivetime")
            new_columns = ("sign", "func", "data", "datetime")
            create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                              `sign` varchar(50) NOT NULL,
                              `funcid` int(11) NOT NULL,
                              `receivetime` datetime NOT NULL,
                              `data` double DEFAULT NULL,
                              `virtual` tinyint(4) DEFAULT '1',
                              PRIMARY KEY (`sign`,`funcid`,`receivetime`),
                              KEY `index_intervaldata` (`sign`,`funcid`,`receivetime`) USING BTREE
                            ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"""
    elif data_type == 2:
        table_name = f"{start_time.year}{start_time.month:02d}_{middlewares}_{database['building_code']}_{freq}"
        columns = ("c_local_id", "c_func", "c_value", "c_receivetime")
        new_columns = ("sign", "func", "data", "datetime")
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                              `c_id` bigint(20) NOT NULL AUTO_INCREMENT,
                              `c_local_id` varchar(50) DEFAULT NULL,
                              `c_func` varchar(30) DEFAULT NULL,
                              `c_value` varchar(30) DEFAULT NULL,
                              `c_receivetime` datetime DEFAULT NULL,
                              `c_type` varchar(30) DEFAULT NULL,
                              `c_remark` varchar(30) DEFAULT NULL,
                              PRIMARY KEY (`c_id`),
                              UNIQUE KEY `index1` (`c_local_id`,`c_func`,`c_receivetime`)
                            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"""
    elif data_type == 3:
        if frequency == 1:
            table_name = f"{start_time.year}_running_{database['building_code']}_{freq}"
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_running_{database['building_code']}_{freq}"
        columns = ("c_local_id", "c_func", "c_value", "c_receivetime")
        new_columns = ("sign", "func", "data", "datetime")
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                                      `c_id` bigint(20) NOT NULL AUTO_INCREMENT,
                                      `c_local_id` varchar(50) DEFAULT NULL,
                                      `c_func` varchar(30) DEFAULT NULL,
                                      `c_value` varchar(30) DEFAULT NULL,
                                      `c_receivetime` datetime DEFAULT NULL,
                                      `c_type` varchar(30) DEFAULT NULL,
                                      `c_remark` varchar(30) DEFAULT NULL,
                                      PRIMARY KEY (`c_id`),
                                      UNIQUE KEY `index1` (`c_local_id`,`c_func`,`c_receivetime`)
                                    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"""
    elif data_type == 4:
        if frequency == 1:
            table_name = f"{start_time.year}_item_{freq}"
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_item_{freq}"
        columns = ("building_id", "code", "func", "data", "date_time")
        new_columns = ("building_id", "sign", "func", "data", "datetime")
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                    `building_id` varchar(50) NOT NULL,
                    `code` varchar(50) NOT NULL,
                    `data` double DEFAULT NULL,
                    `date_time` datetime NOT NULL,
                    `func` varchar(50) NOT NULL,
                    PRIMARY KEY (`building_id`,`code`,`date_time`) USING BTREE,
                    KEY `building_id` (`building_id`) USING BTREE,
                    KEY `code` (`code`) USING BTREE,
                    KEY `date_time` (`date_time`) USING BTREE,
                    KEY `building_id_2` (`building_id`,`code`,`date_time`) USING BTREE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    else:
        table_name = f"recentdatas_{database['building_code']}"
        columns = ("c_local_id", "c_func", "c_value", "c_receivetime")
        new_columns = ("sign", "func", "data", "datetime")
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                                              `c_id` bigint(20) NOT NULL AUTO_INCREMENT,
                                              `c_local_id` varchar(50) DEFAULT NULL,
                                              `c_func` varchar(30) DEFAULT NULL,
                                              `c_value` varchar(30) DEFAULT NULL,
                                              `c_receivetime` datetime DEFAULT NULL,
                                              `c_type` varchar(30) DEFAULT NULL,
                                              `c_remark` varchar(30) DEFAULT NULL,
                                              PRIMARY KEY (`c_id`),
                                              UNIQUE KEY `index1` (`c_local_id`,`c_func`,`c_receivetime`)
                                            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"""
    data_conn.execute(create_sql)
    pd_data = pd_data.drop(columns=['data_type', 'value_type'])
    if data_type == 0:
        pd_data['data_type'] = data_type
        pd_data['value_type'] = value_type
    elif data_type != 4:
        if data_type == 1 and table_name_save:
            pass
        else:
            pd_data = pd_data.drop(columns=["building_id"])
    pd_data = pd_data.reindex(columns=new_columns)
    # pd_data = pd_data.reindex(columns=["sign", "func", "value", "datetime"])
    start = 0
    end = 5000
    while True:
        batch_df = pd_data.iloc[start:end]
        if batch_df.empty:
            break
        data_list = pd_data.astype(str).values.tolist()
        data_str = ','.join([str(tuple(i)) if isinstance(i, list) else str(i) for i in data_list])
        insert_sql = f"""insert into `{table_name}` ({",".join(columns)}) values {data_str}
        on duplicate key update {",".join([f'{i} = values({i})' for i in columns])}"""
        data_conn.execute(insert_sql)
        start += 5000
        end += 5000
    data_conn.dispose()
    return {"code": 200, "msg": "success"}


@app.post("/DataServerLogin", response_model=LoginResponse)
async def data_server_login(request_data: User):
    username = request_data.username
    password = request_data.password
    config_conn = get_connect(**DATABASES["config"])
    sql = f"select password,id from `global_user` where phone='{username}'"
    result = config_conn.execute(sql).fetchone()
    config_conn.dispose()
    if result is None:
        return {"code": 400, "msg": "用户或密码错误"}
    if not check_password(result["password"], password):
        return {"code": 400, "msg": "用户或密码错误"}
    return {"code": 200, "msg": "success", "user_id": result["id"]}


@app.post("/DataSend", response_model=ResponseData)
async def data_send(request_data: DataSend):
    building_id = request_data.building_id
    data_type = request_data.data_type
    value_type = request_data.value_type
    frequency = request_data.frequency
    start_time = request_data.start_time
    end_time = request_data.end_time
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    # if (end_time.year != start_time.year or end_time.month != start_time.month) and frequency != "1":
    #     return {"code": 1007, "msg": "时间间隔不能超过10分钟"}
    sign_list = request_data.sign_list
    func_list = request_data.func_list
    flag = request_data.flag
    table_name_get = request_data.table_name_get
    history = request_data.history
    if data_type not in [0, 1, 2, 3, 4, 5]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if value_type not in [0, 1, 2, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if frequency not in [1, 2, 3, 4, 5]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if value_type == 0 and data_type == 2:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if data_type in [1, 4, 5] and value_type != 0:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    elif data_type == 2 and value_type not in [1, 2, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    elif data_type == 3 and value_type not in [0, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    match frequency:
        case 1:
            freq = "m1"
            if data_type != 3 and data_type != 4:
                return {"code": 1004, "msg": "该类型数据不支持1月频率"}
            if start_time.month != end_time.month:
                return {"code": 1007, "msg": "时间跨度不能超过1个月"}
        case 2:
            freq = "d1"
            if data_type != 3 and data_type != 4:
                return {"code": 1004, "msg": "该类型数据不支持1日频率"}
            if start_time.day != end_time.day:
                return {"code": 1007, "msg": "时间跨度不能超过1天"}
        case 3:
            freq = "h1"
            if data_type != 3 and data_type != 4:
                return {"code": 1004, "msg": "该类型数据不支持1小时频率"}
            if history == "True":
                pass
            elif start_time.hour != end_time.hour:
                return {"code": 1007, "msg": "时间跨度不能超过1小时"}
        case 4:
            freq = "m10"
            if data_type != 2 and data_type != 3:
                return {"code": 1004, "msg": "该类型数据不支持10分钟频率"}
            s_time = start_time - timedelta(minutes=start_time.minute % 10, seconds=start_time.second)
            if history == "True":
                pass
            elif end_time > s_time + timedelta(minutes=10) or start_time.hour != end_time.hour:
                return {"code": 1007, "msg": "时间跨度不能超过10分钟"}
        case 5:
            freq = ""
            if data_type != 1 and data_type != 5:
                return {"code": 1004, "msg": "该类型数据不支持最近一条数据"}
    config_conn = get_connect(**DATABASES["config"])
    if flag == 0:
        database_sql = f"select * from `data_server_config` where building_id='{building_id}' and data_type='{data_type}'"
        match value_type:
            case 1:
                middlewares = "instant"
            case 2:
                middlewares = "accumulate"
            case 3:
                middlewares = "equipment"
        cn = config_conn.execute(database_sql)
        config_conn.dispose()
        database = cn.fetchone()
        if not database:
            return {"code": 1001, "msg": "授权校验失败"}
        data_database = {"username": database["username"], "password": database["password"], "host": database["host"],
                         "port": database["port"], "database": database["database"]}
        data_conn = get_connect(**data_database)
        if data_type == 2 or data_type == 3 or data_type == 5:
            columns = ("c_local_id", "c_func", "c_value", "c_receivetime")
            time_column = "c_receivetime"
            sign_column = "c_local_id"
            func_column = "c_func"
            data_column = "c_value"
            building_id = database["building_code"]
        elif data_type == 1:
            columns = ("sign", "funcid", "data", "receivetime")
            time_column = "receivetime"
            sign_column = "sign"
            func_column = "funcid"
            data_column = "data"
            if table_name_get:
                columns = ("buildingid", "sign", "funcid", "data", "receivetime")
        else:
            columns = ("building_id", "code", "func", "data", "date_time")
            time_column = "date_time"
            sign_column = "code"
            func_column = "func"
            data_column = "data"
        if data_type == 1:
            if table_name_get:
                table_name = table_name_get
            else:
                table_name = f"{start_time.year}{start_time.month:02d}_recorddata_{database['building_id']}"
        elif data_type == 2:
            table_name = f"{start_time.year}{start_time.month:02d}_{middlewares}_{database['building_code']}_{freq}"
        elif data_type == 3:
            if frequency == 1:
                table_name = f"{start_time.year}_running_{database['building_code']}_{freq}"
            else:
                table_name = f"{start_time.year}{start_time.month:02d}_running_{database['building_code']}_{freq}"
        elif data_type == 4:
            if frequency == 1:
                table_name = f"{start_time.year}_item_{freq}"
            else:
                table_name = f"{start_time.year}{start_time.month:02d}_item_{freq}"
        else:
            table_name = f"recentdatas_{database['building_code']}"
        if not data_conn.has_table(table_name):
            return {"code": 1006, "msg": "该时间数据表不存在"}
        sign_str = ','.join([f"'{i}'" for i in sign_list])
        func_str = ','.join([f"'{i}'" for i in func_list])
        data_sql = f"""
        select {",".join(columns)} from `{table_name}` where {time_column} between '{start_time}' and '{end_time}'
        """
        if data_type == 1 and table_name_get:
            data_sql += f" and buildingid='{building_id}'"
        if data_type == 4:
            data_sql += f" and building_id='{building_id}'"
        if sign_list:
            if len(sign_list) == 1:
                data_sql += f" and {sign_column}='{sign_list[0]}'"
            else:
                data_sql += f" and {sign_column} in ({sign_str})"
        if func_list:
            if len(func_list) == 1:
                data_sql += f" and {func_column}='{func_list[0]}'"
            else:
                data_sql += f" and {func_column} in ({func_str})"
        data_df = pd.read_sql(data_sql, data_conn)
        data_df.rename(columns={sign_column: "sign", func_column: "func", data_column: "data", time_column: "datetime"},
                       inplace=True)
        data_df["value_type"] = None
        data_df["data_type"] = None
        if data_type != 4:
            data_df["building_id"] = building_id
        data_conn.dispose()
    elif flag == 1:
        database_sql = f"select * from `data_server_config` where building_id='{building_id}' and data_type='0'"
        cn = config_conn.execute(database_sql)
        config_conn.dispose()
        database = cn.fetchone()
        if not database:
            return {"code": 1001, "msg": "授权校验失败"}
        data_database = {"username": database["username"], "password": database["password"], "host": database["host"],
                         "port": database["port"], "database": database["database"]}
        data_conn = get_connect(**data_database)
        columns = ("building_id", "sign", "func", "data", "datetime", "value_type", "data_type")
        data_sql = f"""
                    select {",".join(columns)} from `completion_data` where  `value_type`='{value_type}'
                    and `data_type`='{frequency}' and `datetime` between '{start_time}' and '{end_time}'
                    """
        if data_type == 4 or data_type == 1:
            data_sql += f" and building_id='{building_id}'"
        else:
            data_sql += f" and building_id='{database['building_code']}'"
        sign_str = ','.join([f"'{i}'" for i in sign_list])
        func_str = ','.join([f"'{i}'" for i in func_list])
        if sign_list:
            if len(sign_list) == 1:
                data_sql += f" and sign='{sign_list[0]}'"
            else:
                data_sql += f" and sign in ({sign_str})"
        if func_list:
            if len(func_list) == 1:
                data_sql += f" and func='{func_list[0]}'"
            else:
                data_sql += f" and func in ({func_str})"
        data_df = pd.read_sql(data_sql, data_conn)
    else:
        return {"code": 1008, "msg": "flag参数错误"}
    return {"code": 200, "msg": "success", "data": data_df.to_dict(orient="records")}


# 生成假数据的接口
@app.post("/FakeData")
async def fake_data(request_data: FakeData):
    building_id = request_data.building_id
    frequency = request_data.frequency
    value_type = request_data.value_type
    data_type = request_data.data_type
    start_time = request_data.start_time
    end_time = request_data.end_time
    sign_list = request_data.sign_list
    func_list = request_data.func_list
    table_name_save = request_data.table_name_save
    size = request_data.size
    rules = request_data.rules
    coiled = request_data.coiled
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    if data_type not in [0, 1, 2, 3, 4, 5]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if value_type not in [0, 1, 2, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if frequency not in [1, 2, 3, 4, 5]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if value_type == 0 and data_type == 2:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    if data_type in [1, 4, 5] and value_type != 0:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    elif data_type == 2 and value_type not in [1, 2, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    elif data_type == 3 and value_type not in [0, 3]:
        return {"code": 1005, "msg": "频率、数据类型、值类型校验错误"}
    config_conn = get_connect(**DATABASES["config"])
    database_sql = f"select * from `data_server_config` where building_id='{building_id}' and data_type='{data_type}'"
    cn = config_conn.execute(database_sql)
    config_conn.dispose()
    database = cn.fetchone()
    if not database:
        return {"code": 1001, "msg": "授权校验失败"}
    match frequency:
        case 1:
            freq = "m1"
            span = "MS"
        case 2:
            freq = "d1"
            span = 'D'
        case 3:
            freq = "h1"
            span = '1h'
        case 4:
            freq = "m10"
            span = '10min'
        case 5:
            freq = ""
            span = "5min"
    match value_type:
        case 1:
            middlewares = "instant"
        case 2:
            middlewares = "accumulate"
        case 3:
            middlewares = "equipment"
    if data_type == 0:
        table_name = "completion_data"
    elif data_type == 1:
        if table_name_save:
            table_name = table_name_save
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_recorddata_{building_id}"
    elif data_type == 2:
        table_name = f"{start_time.year}{start_time.month:02d}_{middlewares}_{database['building_code']}_{freq}"
    elif data_type == 3:
        if frequency == 1:
            table_name = f"{start_time.year}_running_{database['building_code']}_{freq}"
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_running_{database['building_code']}_{freq}"
    elif data_type == 4:
        if frequency == 1:
            table_name = f"{start_time.year}_item_{freq}"
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_item_{freq}"
    else:
        table_name = f"recentdatas_{database['building_code']}"
    data_database = {"username": database["username"], "password": database["password"], "host": database["host"],
                     "port": database["port"], "database": database["database"]}
    data_conn = get_connect(**data_database)
    if not data_conn.has_table(table_name):
        return {"code": 1006, "msg": "该数据表不存在"}
    if sign_list:
        pass
    else:
        sign_list = [random.randint(4200000000, 4300000000) for _ in range(10)]
    if func_list:
        pass
    else:
        func_list = [random.randint(6000000, 7000000) for _ in range(1)]
    df = pd.DataFrame()
    # 生成数据传递参数:`rules:{"range":[1,2],"null":false,"zero":false,"float":5,"negative":false, "growth":false}`
    for sign in sign_list:
        func = random.choice(func_list)
        if end_time:
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            time_series = pd.date_range(start=start_time, end=end_time, freq=span)
        else:
            time_series = pd.date_range(start=start_time, periods=size / len(sign_list), freq=span)
        n = len(time_series)
        if rules:
            data_list = generate_fake_data(n, rules)
        else:
            data_list = [random.randint(0, 100) for _ in range(len(time_series))]
        dict = {"sign": sign, "func": func, "datetime": time_series, "data": data_list}
        df = pd.concat([df, pd.DataFrame(dict)])
    df = df.reset_index(drop=True)
    if not coiled:
        df_drop = df.sample(frac=0.1, replace=False, random_state=1)
        df = pd.concat([df, df_drop]).drop_duplicates(keep=False)
    if data_type == 0:
        df['building_id'] = building_id
        df['data_type'] = data_type
        df['value_type'] = value_type
    elif data_type == 1:
        df.rename(columns={"datetime": "receivetime", "func": "funcid"}, inplace=True)
        if table_name_save:
            df["buildingid"] = building_id
    elif data_type == 2 or data_type == 3 or data_type == 5:
        df.rename(columns={"datetime": "c_receivetime", "func": "c_func", "sign": "c_local_id", "data": "c_value"},
                  inplace=True)
    else:
        df["building_id"] = building_id
        df.rename(columns={"datetime": "date_time", "sign": "code"}, inplace=True)
    df.to_sql(table_name, data_conn, index=False, if_exists="append")
    return {"code": 200, "msg": "success"}

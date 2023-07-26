import base64
import json
import os
import multiprocessing as mp
import numpy as np
import pandas as pd
import requests
import threading

from datetime import datetime
from functools import partial
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
from sqlalchemy import create_engine, inspect
from config_log import Log


# log存储文件夹
log_file_name = "DownloadData_logs"

event = threading.Event()

# 新表需要创建的索引
completion_data_index = [
    "create index building_id_2 on completion_data (sign);",
    "create index building_id_3 on completion_data (building_id);",
    "create index completion_data_data_type_index on completion_data (data_type);",
    "create index completion_data_datetime_index on completion_data (datetime);",
    "create index completion_data_func_index on completion_data (func);",
    "create index completion_data_index on completion_data (building_id, data, data_type, value_type, func, datetime, sign);",
    "create index completion_data_value_type_index on completion_data (value_type);"
]

# 数据存储一次最大条数
max_store_count = 10000


def get_connect(**kwargs):
    user = kwargs.get('username')
    password = kwargs.get('password')
    host = kwargs.get('host')
    port = kwargs.get('port')
    database = kwargs.get('database')
    connect = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password, host, port, database),
                            connect_args={"charset": "utf8"})
    return connect


def encrypt_by_rsa(rsa_data, key_type="pbu_key"):
    if key_type == "pbu_key":
        with open(Path(Path().absolute(), 'pbu_key.txt'), "r") as file_pub:
            key = file_pub.read()
    else:
        with open(Path(Path().absolute(), 'pri_key.txt'), "r") as file_pri:
            key = file_pri.read()
    rsa_key = RSA.importKey(key)
    cipher = PKCS1_v1_5.new(rsa_key)
    text = cipher.encrypt(rsa_data.encode("utf-8"))
    return base64.b64encode(text).decode("utf-8")


def is_exactly(x, minutes):
    return np.abs(x) == pd.to_timedelta(minutes * 60, unit='S')


def round_timestamp(ts):
    floor = ts.floor('10 min')
    rounded = ts.round('10 min')
    if is_exactly(rounded - ts, 5):
        return floor
    else:
        return rounded


def login(url, upload_data, logger):
    password = encrypt_by_rsa(upload_data["password"])
    upload_data["password"] = password
    res = requests.post(url=url, json=upload_data)
    try:
        res_data = res.json()
        if res_data["code"] == 200:
            logger.info("登陆成功")
            return res_data["user_id"]
        else:
            logger.error(res_data["msg"])
            return None
    except Exception as e:
        logger.error(f"登录失败，错误信息：{e}")
        return None


def table_exists(table_name, conn):
    insp = inspect(conn)
    table_names = insp.get_table_names()
    if table_name not in table_names:
        return True
    return False


def create_database_table(database_info, logger, flag, transfer_time):
    data_conn = get_connect(**database_info)
    if table_exists(transfer_time, data_conn):
        table_sql = f"""CREATE TABLE `{transfer_time}` (
                             `id` int(10) NOT NULL AUTO_INCREMENT,
                             `building_id` varchar(255) NOT NULL,
                             `building_code` varchar(255) DEFAULT NULL,
                             `frequency` varchar(10) NOT NULL,
                             `value_type` varchar(10) NOT NULL,
                             `datetime` datetime NOT NULL,
                             `send_or_get` int(1) NOT NULL,
                             PRIMARY KEY (`id`),
                             UNIQUE KEY `building_id` (`building_id`,`frequency`,`value_type`,`send_or_get`) USING BTREE
                           ) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;"""
        try:
            data_conn.execute(table_sql)
        except Exception as e:
            logger.error(f"创建表{transfer_time}失败！失败原因：{e}")
    if flag:
        if table_exists("completion_data", data_conn):
            table_sql = f"""CREATE TABLE `completion_data`(
                                `id` int(64) NOT NULL AUTO_INCREMENT ,
                                `building_id` varchar(64) NOT NULL,
                                `sign` varchar(64) NOT NULL,
                                `func` varchar(20) NOT NULL,
                                `datetime` datetime NOT NULL,
                                `value_type` varchar(20) NOT NULL,
                                `data_type` int(2) NOT NULL,
                                `data` varchar(255) NOT NULL,
                                PRIMARY KEY (`id`),
                                UNIQUE KEY (`building_id`, `sign`, `func`, `datetime`, `value_type`, `data_type`) USING BTREE
                                )ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;"""
            try:
                data_conn.execute(table_sql)
            except Exception as e:
                logger.error(f"创建表completion_data失败！失败原因：{e}")
            for index in completion_data_index:
                table_sql = index
                data_conn.execute(table_sql)
        data_conn.dispose()


def get_config_detail(start_time, building_info, get_config, conn, flag, history=False):
    if get_config["frequency"] == "1":
        freq = "m1"
        start_time = start_time.replace(day=1, hour=0, minute=0, second=0)
        end_time = start_time + pd.DateOffset(months=1) - pd.DateOffset(seconds=1)
    elif get_config["frequency"] == "2":
        freq = "d1"
        start_time = start_time.replace(hour=0, minute=0, second=0)
        end_time = start_time + pd.DateOffset(days=1) - pd.DateOffset(seconds=1)
    elif get_config["frequency"] == "3":
        freq = "h1"
        start_time = start_time.replace(minute=0, second=0)
        if history:
            end_time = start_time.replace(hour=23, minute=59, second=59)
            cur_time = datetime.now()
            if end_time.year == cur_time.year and end_time.month == cur_time.month and end_time.day == cur_time.day:
                end_time = cur_time.replace(minute=0, second=0, microsecond=0) - pd.DateOffset(seconds=1)
        else:
            end_time = start_time + pd.DateOffset(hours=1) - pd.DateOffset(seconds=1)
    elif get_config["frequency"] == "4" or get_config["frequency"] == "5":
        if get_config["frequency"] == "4":
            freq = "m10"
        elif get_config["frequency"] == "5":
            freq = ""
        minute = (start_time.minute // 10) * 10
        start_time = start_time.replace(minute=minute, second=0)
        if history:
            end_time = start_time.replace(hour=23, minute=59, second=59)
            cur_time = datetime.now()
            if end_time.year == cur_time.year and end_time.month == cur_time.month and end_time.day == cur_time.day:
                minute = (cur_time.minute // 10) * 10
                end_time = cur_time.replace(minute=minute, second=0, microsecond=0) - pd.DateOffset(seconds=1)
        else:
            end_time = start_time + pd.DateOffset(minutes=10) - pd.DateOffset(seconds=1)
    else:
        raise Exception("频率、数据类型、值类型校验错误")
    if flag:
        columns = {"building_id": "building_id", "sign": "sign", "func": "func", "data": "data", "datetime": "datetime", \
                   "data_type": "data_type", "value_type": "value_type"}
        table_name = "completion_data"
        detail = {"end_time": end_time, "freq": "", "columns": columns, "building_id": "",
                  "table_name": table_name, "start_time": start_time}
        return detail
    table_name_save = get_config["table_name_save"]
    if get_config["data_type"] == "1":
        if table_name_save != "":
            columns = {"building_id": "buildingid", "sign": "sign", "func": "funcid", "datetime": "receivetime",
                       "data": "data"}
        else:
            columns = {"sign": "sign", "func": "funcid", "datetime": "receivetime", "data": "data"}
        building_id = building_info["building_id"]
    elif get_config["data_type"] == "2" or get_config["data_type"] == "3":
        columns = {"sign": "c_local_id", "func": "c_func", "data": "c_value", "datetime": "c_receivetime"}
        building_id = building_info["building_code"]
    elif get_config["data_type"] == "4":
        columns = {"building_id": "building_id", "sign": "code", "func": "func", "data": "data",
                   "datetime": "date_time"}
        building_id = building_info["building_id"]
    elif get_config["data_type"] == "5":
        columns = {"sign": "c_local_id", "func": "c_func", "datetime": "c_receivetime", "data": "c_value"}
        building_id = building_info["building_code"]
    match get_config["value_type"]:
        case "0":
            middlewares = ""
        case "1":
            middlewares = "instant"
        case "2":
            middlewares = "accumulate"
        case "3":
            if get_config["data_type"] == "2":
                middlewares = "equipment"
            elif get_config["data_type"] == "3":
                middlewares = "running"
        case _:
            raise Exception("频率、数据类型、值类型校验错误")
    if get_config["data_type"] == "1":
        if table_name_save != "":
            table_name = table_name_save
            create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                        `id` int unsigned NOT NULL AUTO_INCREMENT,
                        `buildingid` varchar(255) NOT NULL,
                        `sign` varchar(50) NOT NULL,
                        `funcid` int(11) NOT NULL,
                        `receivetime` datetime NOT NULL,
                        `data` double DEFAULT NULL,
                        PRIMARY KEY (`id`,`sign`,`funcid`,`receivetime`),
                        KEY `index_intervaldata` (`sign`,`funcid`,`receivetime`) USING BTREE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
            if table_exists(table_name, conn):
                conn.execute(create_sql)
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_recorddata_{building_id}"
            create_sql = f"""CREATE TABLE if not exists `{table_name}` (
              `id` int unsigned NOT NULL AUTO_INCREMENT,  
              `sign` varchar(50) NOT NULL,
              `funcid` int(11) NOT NULL,
              `receivetime` datetime NOT NULL,
              `data` double DEFAULT NULL,
              `virtual` tinyint(4) DEFAULT '1',
              PRIMARY KEY (`id`,`sign`,`funcid`,`receivetime`),
              KEY `index_intervaldata` (`sign`,`funcid`,`receivetime`) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
            if table_exists(table_name, conn):
                conn.execute(create_sql)
    elif get_config["data_type"] == "2":
        table_name = f"{start_time.year}{start_time.month:02d}_{middlewares}_{building_id}_{freq}"
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
          `c_id` bigint(20) NOT NULL AUTO_INCREMENT,
          `c_local_id` varchar(50) DEFAULT NULL,
          `c_func` varchar(30) DEFAULT NULL,
          `c_value` varchar(30) DEFAULT NULL,
          `c_receivetime` datetime DEFAULT NULL,
          PRIMARY KEY (`c_id`),
          UNIQUE KEY `index1` (`c_local_id`,`c_func`,`c_receivetime`)
        ) ENGINE=InnoDB AUTO_INCREMENT=14549 DEFAULT CHARSET=utf8;"""
        if table_exists(table_name, conn):
            conn.execute(create_sql)
    elif get_config["data_type"] == "3":
        if get_config["value_type"] == "":
            table_name = f"{start_time.year}{start_time.month:02d}_{building_id}_{freq}"
        else:
            table_name = f"{start_time.year}{start_time.month:02d}_{middlewares}_{building_id}_{freq}"
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
          `c_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `c_local_id` varchar(50) NOT NULL,
          `c_func` varchar(30) NOT NULL,
          `c_value` varchar(30) NOT NULL,
          `c_receivetime` datetime NOT NULL,
          PRIMARY KEY (`c_id`),
          UNIQUE KEY `index_name` (`c_local_id`,`c_func`,`c_receivetime`) USING BTREE
        ) ENGINE=InnoDB AUTO_INCREMENT=923019 DEFAULT CHARSET=utf8;"""
        if table_exists(table_name, conn):
            conn.execute(create_sql)
    elif get_config["data_type"] == "4":
        table_name = f"{start_time.year}{start_time.month:02d}_item_{freq}"
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                  `c_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
                  `building_id` varchar(255) NOT NULL,
                  `code` varchar(50) NOT NULL,
                  `func` varchar(30) NOT NULL,
                  `data` varchar(30) NOT NULL,
                  `date_time` datetime NOT NULL,
                  PRIMARY KEY (`c_id`),
                  UNIQUE KEY `index_name` (`code`,`func`,`date_time`, `building_id`) USING BTREE
                ) ENGINE=InnoDB AUTO_INCREMENT=923019 DEFAULT CHARSET=utf8;"""
        if table_exists(table_name, conn):
            conn.execute(create_sql)
    elif get_config["data_type"] == "5":
        table_name = f"recentdatas_{building_id}"
        create_sql = f"""CREATE TABLE if not exists `{table_name}` (
                            `id` int unsigned NOT NULL AUTO_INCREMENT, 
                            `c_local_id` varchar(50) NOT NULL,
                            `c_func` int(11) NOT NULL,
                            `c_receivetime` datetime NOT NULL,
                            `c_value` double DEFAULT NULL,
                            PRIMARY KEY (`id`,`c_local_id`,`c_func`,`c_receivetime`),
                            KEY `index_intervaldata` (`c_local_id`,`c_func`,`c_receivetime`) USING BTREE
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        if table_exists(table_name, conn):
            conn.execute(create_sql)
    else:
        raise Exception("频率、数据类型、值类型校验错误")
    if table_name_save != "" and get_config["data_type"] == "1":
        detail = {"end_time": end_time, "freq": freq, "columns": columns, "building_id": building_id,
                  "table_name": table_name, "table_name_save": table_name_save, "start_time": start_time}
    else:
        detail = {"end_time": end_time, "freq": freq, "columns": columns, "building_id": building_id,
                  "table_name": table_name, "start_time": start_time}
    return detail


def get_data(claim_data, user_id, building_id, url, logger):
    check_data = {"building_id": building_id, "user_id": user_id}
    token = encrypt_by_rsa(str(check_data))
    headers = {"Authorization": token}
    while True:
        try:
            res = requests.post(url=url, json=claim_data, headers=headers)
            break
        except requests.exceptions.ConnectionError:
            logger.warning("网络连接失败，等待10s后重新连接")
            event.wait(10)
    try:
        res_data = res.json()
        if res_data["code"] == 200:
            logger.info(
                f'获取数据成功:{building_id}: {claim_data["start_time"]}--{claim_data["end_time"]}--{len(res_data["data"])}')
            data = res_data["data"]
            return data
        else:
            logger.error("{}:{}".format(building_id, res_data["msg"]))
            return None
    except Exception as e:
        logger.error(f"获取数据失败{building_id}，错误信息：{e}")
        return None


def get_data_local(get_config, building_info, data_conn, start_time, end_time, flag, logger):
    value_type = get_config["value_type"]
    building_id = building_info["building_id"]
    func_list = building_info["func_list"]
    sign_list = building_info["sign_list"]
    match get_config["frequency"]:
        case "1":
            freq = "m1"
        case "2":
            freq = "d1"
        case "3":
            freq = "h1"
        case "4":
            freq = "m10"
        case "5":
            freq = ""
    if flag == 0:
        match get_config["value_type"]:
            case "1":
                middlewares = "instant"
            case "2":
                middlewares = "accumulate"
            case "3":
                middlewares = "equipment"
        if get_config["data_type"] == "2" or get_config["data_type"] == "3" or get_config["data_type"] == "5":
            columns = ("c_local_id", "c_func", "c_value", "c_receivetime")
            time_column = "c_receivetime"
            sign_column = "c_local_id"
            func_column = "c_func"
            data_column = "c_value"
            building_id = building_info["building_code"]
        elif get_config["data_type"] == "1":
            columns = ("sign", "funcid", "data", "receivetime")
            time_column = "receivetime"
            sign_column = "sign"
            func_column = "funcid"
            data_column = "data"
            if get_config["table_name_get"]:
                columns = ("buildingid", "sign", "funcid", "data", "receivetime")
        else:
            columns = ("building_id", "code", "func", "data", "date_time")
            time_column = "date_time"
            sign_column = "code"
            func_column = "func"
            data_column = "data"
        if get_config["data_type"] == "1":
            if get_config["table_name_get"]:
                table_name = get_config["table_name_get"]
            else:
                table_name = f"{start_time.year}{start_time.month:02d}_recorddata_{building_info['building_id']}"
        elif get_config["data_type"] == "2":
            table_name = f"{start_time.year}{start_time.month:02d}_{middlewares}_{building_info['building_code']}_{freq}"
        elif get_config["data_type"] == "3":
            if get_config["frequency"] == "1":
                table_name = f"{start_time.year}_running_{building_info['building_code']}_{freq}"
            else:
                table_name = f"{start_time.year}{start_time.month:02d}_running_{building_info['building_code']}_{freq}"
        elif get_config["data_type"] == "4":
            if get_config["frequency"] == "1":
                table_name = f"{start_time.year}_item_{freq}"
            else:
                table_name = f"{start_time.year}{start_time.month:02d}_item_{freq}"
        else:
            table_name = f"recentdatas_{building_info['building_code']}"
        if not data_conn.has_table(table_name):
            logger.error(f"获取数据失败{building_id}，错误信息：该时间数据表不存在")
            return None
        sign_str = ','.join([f"'{i}'" for i in building_info["sign_list"]])
        func_str = ','.join([f"'{i}'" for i in building_info["func_list"]])
        data_sql = f"""
            select {",".join(columns)} from `{table_name}` where {time_column} between '{start_time}' and '{end_time}'
            """
        if get_config["data_type"] == "1" and get_config["table_name_get"]:
            data_sql += f" and buildingid='{building_id}'"
        if get_config["data_type"] == "4":
            data_sql += f" and building_id='{building_id}'"
        if building_info["sign_list"]:
            if len(building_info["sign_list"]) == 1:
                data_sql += f" and {sign_column}='{sign_list[0]}'"
            else:
                data_sql += f" and {sign_column} in ({sign_str})"
        if building_info["func_list"]:
            if len(building_info["func_list"]) == 1:
                data_sql += f" and {func_column}='{func_list[0]}'"
            else:
                data_sql += f" and {func_column} in ({func_str})"
        data_df = pd.read_sql(data_sql, data_conn)
        data_df.rename(columns={sign_column: "sign", func_column: "func", data_column: "data", time_column: "datetime"},
                       inplace=True)
        data_df["value_type"] = None
        data_df["data_type"] = None
        if get_config["data_type"] != "4":
            data_df["building_id"] = building_id
        data_conn.dispose()
    elif flag == 1:
        columns = ("building_id", "sign", "func", "data", "datetime", "value_type", "data_type")
        data_sql = f"""
                        select {",".join(columns)} from `completion_data` where  `value_type`='{value_type}'
                        and `data_type`='{get_config["frequency"]}' and `datetime` between '{start_time}' and '{end_time}'
                        """
        if get_config["data_type"] == "4" or get_config["data_type"] == "1":
            data_sql += f" and building_id='{building_id}'"
        else:
            data_sql += f" and building_id='{building_info['building_code']}'"
        sign_str = ','.join([f"'{i}'" for i in building_info["sign_list"]])
        func_str = ','.join([f"'{i}'" for i in building_info["func_list"]])
        if building_info["sign_list"]:
            if len(building_info["sign_list"]) == 1:
                data_sql += f" and sign='{sign_list[0]}'"
            else:
                data_sql += f" and sign in ({sign_str})"
        if building_info["func_list"]:
            if len(building_info["func_list"]) == 1:
                data_sql += f" and func='{func_list[0]}'"
            else:
                data_sql += f" and func in ({func_str})"
        data_df = pd.read_sql(data_sql, data_conn)
    res = data_df.to_dict(orient="records")
    if res:
        logger.info(
            f'获取数据成功:{building_id}: {start_time}--{end_time}--{len(res)}')
    return res


def format_data(data, columns):
    df = pd.DataFrame(data, columns=columns)
    data_list = df.values.tolist()
    data_list = [str(tuple(data)) for data in data_list]
    data_str = ",".join(data_list)
    return data_str


def method_name(data_conn, end_time, get_config, transfer_time):
    start_time = end_time + pd.DateOffset(seconds=1)
    update_sql = f"""insert into `{transfer_time}` (building_id,building_code,frequency,value_type,datetime,send_or_get) 
                    values ('{get_config['building_id']}','{get_config['building_code']}','{get_config['frequency']}','{get_config['value_type']}','{start_time}',0)
                    on duplicate key update datetime='{start_time}'"""
    data_conn.execute(update_sql)
    return start_time


def store_data(data_str, conn, columns, table_name):
    insert_sql = f"""insert into `{table_name}` ({",".join(columns)}) values {data_str}
    on duplicate key update {",".join([f'{i} = values({i})' for i in columns])}"""
    conn.execute(insert_sql)


def thread_func(building_info, logger, user_id, default_time, url, get_config, data_conn, flag, transfer_time,
                use_local, read_data_conn, wait_time, skip_time):
    building_id = building_info["building_id"]
    sql = f"""select * from `{transfer_time}` where building_id = '%s' and frequency = '%s' and value_type = '%s' and send_or_get=0""" % (
        building_id, get_config["frequency"], get_config["value_type"])
    res = data_conn.execute(sql)
    res = res.fetchall()
    if res:
        start_time = res[0]["datetime"]
    else:
        start_time = datetime.strptime(default_time, "%Y-%m-%d %H:%M:%S")
    old_time = start_time
    while start_time < datetime.now():
        cur_time = datetime.now()
        if get_config["frequency"] == "1":
            if cur_time.year == start_time.year and cur_time.month == start_time.month:
                break
        elif get_config["frequency"] == "2":
            if cur_time.year == start_time.year and cur_time.month == start_time.month and cur_time.day == start_time.day:
                break
        elif get_config["frequency"] == "3":
            if cur_time.year == start_time.year and cur_time.month == start_time.month and \
                    cur_time.day == start_time.day and cur_time.hour == start_time.hour:
                break
        elif get_config["frequency"] == "4" or get_config["frequency"] == "5":
            if cur_time.year == start_time.year and cur_time.month == start_time.month and \
                    cur_time.day == start_time.day and cur_time.hour == start_time.hour and cur_time.minute // 10 == start_time.minute // 10:
                break
        detail = get_config_detail(start_time, building_info, get_config, data_conn, flag, True)
        start_time = detail["start_time"]
        end_time = detail["end_time"]
        columns_format = tuple(detail["columns"].keys())
        columns_store = tuple(detail["columns"].values())
        table_name = detail["table_name"]
        merge_config = dict(building_info, **get_config)
        post_data = dict(merge_config, **{"start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                          "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")})
        post_data.update({"flag": flag})
        if get_config["frequency"] == "3" or get_config["frequency"] == "4":
            post_data.update({"history": "True"})
        if use_local:
            res = get_data_local(get_config, building_info, read_data_conn, start_time, end_time, flag, logger)
        else:
            res = get_data(post_data, user_id, building_id, url, logger)
        if res is None or len(res) <= max_store_count:
            res = [res]
        else:
            res = [res[i: i + max_store_count] for i in range(0, len(res), max_store_count)]
        for r in res:
            if r:
                format_res = format_data(r, columns_format)
                for _ in range(3):
                    try:
                        store_data(format_res, data_conn, columns_store, table_name)
                        break
                    except Exception as e:
                        logger.error(f"存储数据失败:{building_id}，错误信息：{e}")
                        event.wait(5)
            else:
                logger.warning("该时间段数据为空:{}，获取下一时间段数据".format(building_id))
                event.wait(600)
        start_time = method_name(data_conn, end_time, merge_config, transfer_time)
        old_time = start_time
    start_time = old_time
    end_time = start_time - pd.DateOffset(seconds=1)
    first_flag = True
    wait_flag = False
    while True:
        if get_config["frequency"] == "1" or get_config["frequency"] == "2":
            if first_flag:
                first_flag = False
                sleep_time = (datetime.now().replace(hour=23, minute=59, second=59,
                                                     microsecond=0) - datetime.now()).seconds
                if wait_time:
                    if wait_time * 60 < sleep_time:
                        sleep_time = wait_time * 60
                        wait_flag = True
                logger.info("休眠{}分钟执行{}".format(sleep_time//60, building_id))
                event.wait(sleep_time)
            else:
                if wait_time:
                    if wait_time * 60 < 3600 * 24:
                        cur_time = datetime.now().replace(microsecond=0)
                        next_time = cur_time.replace(hour=0, minute=0, second=0, microsecond=0) + pd.DateOffset(
                            days=1) - pd.DateOffset(seconds=1)
                        if cur_time + pd.DateOffset(minutes=wait_time) > next_time:
                            sleep_time = (next_time - cur_time).seconds
                            if sleep_time != 0:
                                sleep_time = sleep_time
                            else:
                                sleep_time = wait_time * 60
                        else:
                            sleep_time = wait_time * 60
                        wait_flag = True
                        logger.info("休眠{}分钟执行{}".format(sleep_time//60, building_id))
                    else:
                        sleep_time = 3600 * 24
                        logger.info("休眠一天执行{}".format(building_id))
                else:
                    sleep_time = 3600 * 24
                    logger.info("休眠一天执行{}".format(building_id))
                event.wait(sleep_time)
        elif get_config["frequency"] == "3":
            if first_flag:
                first_flag = False
                sleep_time = (datetime.now().replace(minute=59, second=59, microsecond=0) - datetime.now()).seconds
                if wait_time:
                    if wait_time * 60 < sleep_time:
                        sleep_time = wait_time * 60
                        wait_flag = True
                logger.info("休眠{}分钟执行{}".format(sleep_time//60, building_id))
                event.wait(sleep_time)
            else:
                if wait_time:
                    if wait_time * 60 < 3600:
                        cur_time = datetime.now().replace(microsecond=0)
                        next_time = cur_time.replace(minute=59, second=59, microsecond=0)
                        if cur_time + pd.DateOffset(minutes=wait_time) > next_time:
                            sleep_time = (next_time - cur_time).seconds
                            if sleep_time != 0:
                                sleep_time = sleep_time
                            else:
                                sleep_time = wait_time * 60
                        else:
                            sleep_time = wait_time * 60
                        wait_flag = True
                        logger.info("休眠{}分钟执行{}".format(sleep_time//60, building_id))
                    else:
                        sleep_time = 3600
                        logger.info("休眠一小时执行{}".format(building_id))
                else:
                    sleep_time = 3600
                    logger.info("休眠一小时执行{}".format(building_id))
                event.wait(sleep_time)
        elif get_config["frequency"] == "4":
            if first_flag:
                first_flag = False
                cur_time = datetime.now().replace(microsecond=0)
                minute = (cur_time.minute // 10) * 10 + 10
                if minute > 50:
                    minute = 59
                    next_time = cur_time.replace(minute=minute, second=59, microsecond=0)
                else:
                    next_time = cur_time.replace(minute=minute, second=0, microsecond=0) - pd.DateOffset(seconds=1)
                sleep_time = (next_time - cur_time).seconds
                if wait_time:
                    if wait_time * 60 < sleep_time:
                        sleep_time = wait_time * 60
                        wait_flag = True
                logger.info("休眠{}分钟执行{}".format(sleep_time//60, building_id))
                event.wait(sleep_time)
            else:
                if wait_time:
                    if wait_time * 60 < 600:
                        cur_time = datetime.now().replace(microsecond=0)
                        minute = (cur_time.minute // 10) * 10 + 10
                        if minute > 50:
                            minute = 59
                            next_time = cur_time.replace(minute=minute, second=59, microsecond=0)
                        else:
                            next_time = cur_time.replace(minute=minute, second=0, microsecond=0) - pd.DateOffset(
                                seconds=1)
                        if cur_time + pd.DateOffset(minutes=wait_time) > next_time:
                            sleep_time = (next_time - cur_time).seconds
                            if sleep_time != 0:
                                sleep_time = sleep_time
                            else:
                                sleep_time = wait_time * 60
                        else:
                            sleep_time = wait_time * 60
                        wait_flag = True
                        logger.info("休眠{}分钟执行{}".format(sleep_time//60, building_id))
                    else:
                        sleep_time = 600
                        logger.info("休眠10分钟执行{}".format(building_id))
                else:
                    sleep_time = 600
                    logger.info("休眠10分钟执行{}".format(building_id))
                event.wait(sleep_time)
        elif get_config["frequency"] == "5":
            if wait_time:
                if wait_time < 10:
                    cur_time = datetime.now().replace(microsecond=0)
                    minute = (cur_time.minute // 10) * 10 + 10
                    if minute > 50:
                        minute = 59
                        next_time = cur_time.replace(minute=minute, second=59, microsecond=0)
                    else:
                        next_time = cur_time.replace(minute=minute, second=0, microsecond=0) - pd.DateOffset(
                            seconds=1)
                    if cur_time + pd.DateOffset(minutes=wait_time) > next_time:
                        sleep_time = (next_time - cur_time).seconds
                        if sleep_time != 0:
                            sleep_time = sleep_time
                        else:
                            sleep_time = wait_time * 60
                    else:
                        sleep_time = wait_time * 60
                    wait_flag = True
                    logger.info("休眠{}分钟执行{}".format(sleep_time//60, building_id))
                    event.wait(sleep_time)
        old_end_time = end_time
        if wait_flag:
            if start_time > datetime.now():
                start_time = old_end_time
        else:
            if get_config["frequency"] == "1" or get_config["frequency"] == "5":
                if start_time > datetime.now():
                    start_time = old_end_time
        detail = get_config_detail(start_time, building_info, get_config, data_conn, flag)
        start_time = detail["start_time"]
        end_time = detail["end_time"]
        columns_format = tuple(detail["columns"].keys())
        columns_store = tuple(detail["columns"].values())
        table_name = detail["table_name"]
        merge_config = dict(building_info, **get_config)
        post_data = dict(merge_config, **{"start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                          "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")})
        post_data.update({"flag": flag})
        if use_local:
            res = get_data_local(get_config, building_info, read_data_conn, start_time, end_time, flag, logger)
        else:
            res = get_data(post_data, user_id, building_id, url, logger)
        if res is None or len(res) <= max_store_count:
            res = [res]
        else:
            res = [res[i: i + max_store_count] for i in range(0, len(res), max_store_count)]
        for r in res:
            if r:
                format_res = format_data(r, columns_format)
                for _ in range(3):
                    try:
                        store_data(format_res, data_conn, columns_store, table_name)
                        break
                    except Exception as e:
                        logger.error(f"存储数据失败:{building_id}，错误信息：{e}")
                        event.wait(5)
                start_time = method_name(data_conn, end_time, merge_config, transfer_time)
            else:
                if end_time + pd.DateOffset(minutes=skip_time) < datetime.now():
                    logger.error("已超过跳过时间段仍没有数据:{}，获取下一个时间段数据".format(building_id))
                    start_time = method_name(data_conn, end_time, merge_config, transfer_time)
                    continue
                logger.warning("该时间段数据为空:{}，再次尝试获取数据".format(building_id))


def data_transfer(building_info_list, user_id, default_time, url, get_config, database_info, flag, transfer_time,
                  use_local, local_database, wait_time, skip_time):
    data_conn = get_connect(**database_info)
    read_data_conn = get_connect(**local_database)
    part_thread_func = partial(thread_func, user_id=user_id, default_time=default_time, url=url,
                               get_config=get_config, data_conn=data_conn, flag=flag, transfer_time=transfer_time,
                               use_local=use_local, read_data_conn=read_data_conn,
                               wait_time=wait_time, skip_time=skip_time)
    executor = ThreadPoolExecutor(max_workers=20)
    thread_list = []
    for building_info in building_info_list:
        building_log = Log().get_logger(building_info["building_id"], log_file_name)
        thrd = executor.submit(part_thread_func, building_info, building_log)
        thread_list.append(thrd)


def main():
    with open(os.path.join(os.path.dirname(__file__), 'get_data_config.json'), 'r', encoding="utf-8") as f:
        config = json.load(f)
    main_logger = Log().get_logger("main", log_file_name)
    user_id = login(config["login_url"], config["user_info"], main_logger)
    if not user_id:
        return False
    create_database_table(config["database_info"], main_logger, config["flag"], config["data_transfer_time"])
    max_workers = config["max_workers"]
    pool = mp.Pool(processes=max_workers)
    part_data_transfer = partial(data_transfer, user_id=user_id, default_time=config["default_time"],
                                 url=config["get_url"],
                                 get_config=config["get_config"], database_info=config["database_info"],
                                 flag=config["flag"],
                                 transfer_time=config["data_transfer_time"], use_local=config["use_local"],
                                 local_database=config["local_database"],
                                 wait_time=config["wait_time"], skip_time=config["skip_time"])
    if len(config["building_info_list"]) > max_workers:
        building_info_list = [config["building_info_list"][i:i + len(config["building_info_list"]) // max_workers] for i
                              in range(0, len(config["building_info_list"]),
                                       len(config["building_info_list"]) // max_workers)]
    else:
        building_info_list = [config["building_info_list"]]
    try:
        pool.map(part_data_transfer, building_info_list)
    except Exception as e:
        main_logger.error(f"程序异常，错误信息：{e}\n请手动重启程序")
    finally:
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()

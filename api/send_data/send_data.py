import base64
import time
from pathlib import Path
import logging
import logging.config
import pandas as pd
import requests
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
import os
import json
from datetime import datetime
import multiprocessing as mp
from sqlalchemy import create_engine, inspect
from functools import partial
from logging.handlers import RotatingFileHandler, QueueHandler
from multiprocessing import Queue


class Logger:
    """
    日志模块
    """

    def __init__(self):
        self.err_logger = logging.getLogger("err_logger")
        self.mp_logger = logging.getLogger("mp_logger")
        self.mp_init_flag: bool = False
        self.init_logging()
        return

    def init_logging(self):
        """
        初始化日志
        :return:
        """
        self.create_log_dir_and_file()
        formatter: logging.Formatter = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %("
                                                         "message)s")
        rotating_file_handler = RotatingFileHandler(filename="./logs/info.log", maxBytes=1000000,
                                                    backupCount=100, encoding="utf-8")
        rotating_file_handler.setLevel(level=logging.INFO)  # 保存到日志文件的日志等级

        # 错误日志记录器的初始化配置
        err_logger_rotating_file_handler = RotatingFileHandler(filename="./logs/error.log",
                                                               maxBytes=1000000, backupCount=100, encoding="utf-8")
        err_logger_rotating_file_handler.setFormatter(formatter)  # 保存到日志文件的日志等级
        self.err_logger.setLevel(logging.WARNING)
        self.err_logger.addHandler(err_logger_rotating_file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)  # 打印在命令行的日志等级
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s",
            handlers=[rotating_file_handler, stream_handler]
        )
        asyncqt_logger = logging.getLogger('asyncqt')
        asyncqt_logger.setLevel(logging.WARNING)

        logging.info("初始化日志完成。")
        return

    def create_log_dir_and_file(self):
        """
        创建日志文件夹及文件
        :return:
        """
        base_path: str = os.getcwd()
        log_path: str = os.path.join(base_path, "logs")
        log_file_path: str = os.path.join(log_path, "info.log").replace("\\", "/")
        err_log_file_path: str = os.path.join(log_path, "error.log").replace("\\", "/")
        self.init_dir(path=log_path)
        if not os.path.exists(log_file_path):
            with open(log_file_path, mode="w", encoding="utf-8") as f:
                pass
        if not os.path.exists(err_log_file_path):
            with open(err_log_file_path, mode="w", encoding="utf-8") as f:
                pass
        return

    @staticmethod
    def init_dir(path):
        """
        初始化文件夹
        :param path:
        :return:
        """
        if not os.path.exists(path):
            os.makedirs(path)
        return

    def init_mp_logger(self, queue: Queue):
        """
        初始化多进程日志
        """
        formatter: logging.Formatter = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %("
                                                         "message)s")
        # 多进程日志记录器的初始化配置
        mp_logger_queue_handler = QueueHandler(queue=queue)
        mp_logger_queue_handler.setFormatter(formatter)
        self.mp_logger.setLevel(logging.WARNING)
        self.mp_logger.addHandler(mp_logger_queue_handler)
        self.mp_init_flag: bool = True
        return


send_logger: Logger = Logger()


def get_connect(**kwargs):
    user = kwargs.get('username')
    password = kwargs.get('password')
    host = kwargs.get('host')
    port = kwargs.get('port')
    database = kwargs.get('database')
    connect = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password, host, port, database))
    return connect


def encrypt_by_rsa(rsa_data, key_type="pbu_key"):
    if key_type == "pbu_key":
        with open(Path(Path().absolute(), 'pbu_key.txt'), "r") as file_pub:
            key = file_pub.read()
    else:
        with open(Path(Path().absolute(), '../../pri_key.txt'), "r") as file_pri:
            key = file_pri.read()
    rsa_key = RSA.importKey(key)
    cipher = PKCS1_v1_5.new(rsa_key)
    text = cipher.encrypt(rsa_data.encode("utf-8"))
    return base64.b64encode(text).decode("utf-8")


def prepare_data(data_conn, start_time, end_time, send_config, table_names):
    if send_config["frequency"] == "1":
        freq = "m1"
    elif send_config["frequency"] == "2":
        freq = "d1"
    elif send_config["frequency"] == "3":
        freq = "h1"
    elif send_config["frequency"] == "4":
        freq = "m10"
    elif send_config["frequency"] == "5" or send_config["frequency"] == "0":
        freq = ""
    else:
        raise Exception("频率、数据类型、值类型校验错误")
    match send_config["value_type"]:
        case "1":
            if send_config["data_type"] == "1":
                middlewares = "recorddata"
            elif send_config["data_type"] == "2":
                middlewares = "instant"
            elif send_config["data_type"] == "3":
                middlewares = "running"
            else:
                raise Exception("频率、数据类型、值类型校验错误")
        case "2":
            if send_config["data_type"] == "1":
                middlewares = "recorddata"
            elif send_config["data_type"] == "2":
                middlewares = "accumulate"
            elif send_config["data_type"] == "3":
                middlewares = "running"
            else:
                raise Exception("频率、数据类型、值类型校验错误")
        case _:
            raise Exception("频率、数据类型、值类型校验错误")
    if send_config["data_type"] == "2" or send_config["data_type"] == "3":
        columns = ("c_local_id", "c_func", "c_value", "c_receivetime")
        time_column = "c_receivetime"
        sign_column = "c_local_id"
        func_column = "c_func"
        data_column = "c_value"
        building_id = send_config["building_code"]
    else:
        columns = ("sign", "funcid", "data", "receivetime")
        time_column = "receivetime"
        sign_column = "sign"
        func_column = "funcid"
        data_column = "data"
        building_id = send_config["building_id"]
    table_name = f"{start_time.year}{start_time.month:02d}_{middlewares}_{building_id}_{freq}"
    if table_name not in table_names:
        send_logger.mp_logger.info(f"该时间没有表{table_name}")
        return None
    sql = f"""select {','.join(columns)} from `{table_name}` where {time_column} >= '{start_time}'
     and {time_column} < '{end_time}'"""
    data_df = pd.read_sql(sql, data_conn)
    if data_df.empty:
        send_logger.mp_logger.info(f"{start_time}-{end_time}没有数据")
        return None
    data_df.rename(columns={sign_column: "sign", func_column: "func", data_column: "data", time_column: "datetime"},
                   inplace=True)
    data_df["datetime"] = data_df["datetime"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    return dict({"data": data_df.to_dict(orient="records")}, **send_config)


def send_data(upload_data, user_id, building_id, url):
    check_data = {"building_id": building_id, "user_id": user_id}
    token = encrypt_by_rsa(str(check_data))
    headers = {"Authorization": token}
    res = requests.post(url=url, json=upload_data, headers=headers)
    try:
        res_data = res.json()
        if res_data["code"] == 200:
            send_logger.mp_logger.info("发送数据成功")
            return True
        else:
            send_logger.mp_logger.error(res_data["msg"])
            return False
    except Exception as e:
        send_logger.mp_logger.error(f"发送数据失败，错误信息：{e}")
        return False


def get_end_time(start_time, frequency):
    if frequency == "1":
        end_time = start_time + pd.DateOffset(months=1)
    elif frequency == "2":
        end_time = start_time + pd.DateOffset(days=1)
    elif frequency == "3":
        end_time = start_time + pd.DateOffset(hours=1)
    elif frequency == "4":
        end_time = start_time + pd.DateOffset(minutes=10)
    elif frequency == "5" or frequency == "0":
        end_time = start_time + pd.DateOffset(minutes=10)
    else:
        raise Exception("频率、数据类型、值类型校验错误")
    if frequency != "1" and end_time.month != start_time.month:
        end_time = end_time - pd.DateOffset(seconds=1)
    return datetime(end_time.year, end_time.month, end_time.day, end_time.hour, end_time.minute)


def data_transfer(send_config, user_id, default_time, url):
    database_info = send_config.pop("database_info")
    building_id = send_config["building_id"]
    data_conn = get_connect(**database_info)
    insp = inspect(data_conn)
    table_names = insp.get_table_names()
    if "data_transfer_time" not in table_names:
        table_sql = f"""CREATE TABLE `data_transfer_time` (
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
        data_conn.execute(table_sql)
    sql = "select * from `data_transfer_time` where building_id = '%s' and frequency = '%s' and value_type = '%s' and send_or_get=1" % (
        send_config["building_id"], send_config["frequency"], send_config["value_type"])
    res = data_conn.execute(sql)
    res = res.fetchall()
    if res:
        start_time = res[0]["datetime"]
    else:
        start_time = datetime.strptime(default_time, "%Y-%m-%d %H:%M:%S")
    while start_time < datetime.now():
        end_time = get_end_time(start_time, send_config["frequency"])
        try:
            data = prepare_data(data_conn, start_time, end_time, send_config, table_names)
        except Exception as e:
            send_logger.mp_logger.error(f"获取数据失败，错误信息：{e}")
            time.sleep(600)
            continue
        if not data:
            start_time = method_name(data_conn, end_time, send_config)
            continue
        res = send_data(data, user_id, building_id, url)
        if res:
            start_time = method_name(data_conn, end_time, send_config)
        else:
            send_logger.mp_logger.error("上传数据失败，休眠10分钟再次尝试")
            time.sleep(600)
    while True:
        if send_config["frequency"] == "1" or send_config["frequency"] == "2":
            send_logger.mp_logger.info("休眠24小时执行")
            time.sleep(3600 * 24)
        elif send_config["frequency"] == "3":
            send_logger.mp_logger.info("休眠1小时执行")
            time.sleep(3600)
        else:
            send_logger.mp_logger.info("休眠10分钟执行")
            time.sleep(600)
        end_time = get_end_time(start_time, send_config["frequency"])
        try:
            data = prepare_data(data_conn, start_time, end_time, send_config)
        except Exception as e:
            send_logger.mp_logger.error(f"获取数据失败，错误信息：{e}")
            time.sleep(600)
            continue
        if not data:
            start_time = method_name(data_conn, end_time, send_config)
        res = send_data(data, user_id, building_id, url)
        if res:
            start_time = method_name(data_conn, end_time, send_config)
        else:
            send_logger.mp_logger.error("上传数据失败，休眠10分钟再次尝试")
            time.sleep(600)


def method_name(data_conn, end_time, send_config):
    start_time = end_time
    update_sql = f"""insert into `data_transfer_time` (building_id,building_code,frequency,value_type,datetime,send_or_get) 
                    values ('{send_config['building_id']}','{send_config['building_code']}','{send_config['frequency']}','{send_config['value_type']}','{start_time}',1)
                    on duplicate key update datetime='{start_time}'"""
    data_conn.execute(update_sql)
    return start_time


def main():
    with open(os.path.join(os.path.dirname(__file__), 'send_data_config.json'), 'r') as f:
        config = json.load(f)
    user_id = login(config)
    if not user_id:
        return False
    max_workers = config["max_workers"]
    pool = mp.Pool(processes=max_workers)
    part_data_transfer = partial(data_transfer, user_id=user_id, default_time=config["default_time"],
                                 url=config["send_url"])
    try:
        pool.map(part_data_transfer, config["send_config"])
    except Exception as e:
        send_logger.mp_logger.error(f"程序异常，错误信息：{e}\n请手动重启程序")
    finally:
        pool.close()
        pool.join()


def login(config):
    url = config["login_url"]
    upload_data = config["user_info"]
    password = encrypt_by_rsa(upload_data["password"])
    upload_data["password"] = password
    res = requests.post(url=url, json=upload_data)
    try:
        res_data = res.json()
        if res_data["code"] == 200:
            send_logger.mp_logger.info("登录成功")
            return res_data["user_id"]
        else:
            send_logger.mp_logger.error(res_data["msg"])
            return None
    except Exception as e:
        send_logger.mp_logger.error(f"登录失败，错误信息：{e}")
        return None


if __name__ == '__main__':
    # data = {"data": [{"sign": "1001", "datetime": "2021-04-01 00:00:00", "func": "6000494", "value": "200"},
    #                  {"sign": "1001", "datetime": "2021-04-01 00:10:00", "func": "6000494", "value": "250"},
    #                  {"sign": "1002", "datetime": "2021-04-01 00:10:00", "func": "6000494", "value": "30"}],
    #         "building_id": "3701022001", "frequency": 4, "value_type": 2, "data_type": 2}
    # with open(os.path.join(os.path.dirname(__file__), 'send_data_config.json'), 'r') as f:
    #     config = json.load(f)
    # send_data(data, "61-11640454247002112", config)
    main()

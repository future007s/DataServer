import base64
import datetime
import hashlib
import hmac
import json
import time
from pathlib import Path
import random

import pandas as pd
import numpy as np
from Crypto.Cipher import AES, PKCS1_v1_5
import pymysql
import redis
import requests
from Crypto.PublicKey import RSA
from sqlalchemy import create_engine


class BaseFunction(object):

    def save_csv(self, data):
        # 字典中的key值即为csv中列名
        dataframe = pd.DataFrame({'id': data['id'], 'name': data['name'], 'type': data['type']})
        # 将DataFrame存储为csv,index表示是否显示行名，default=True
        dataframe.to_csv("meter.csv", index=True, sep=',')

    def __init__(self):
        self.ctx = {'version': '1.0', 'result': 200, 'content': []}
        self.redis_conn = redis.Redis(host='server_redis', port=63079, password='Bnse225788', db=1)
        self.unpad = lambda s: s[:-ord(s[len(s) - 1:])]
        self.sign_key = 'w0GvhJCsIUCdXYIY'.encode("utf-8")
        self.data_secret = '6JnlpX3Vvqh2rSyP'
        self.data_secret_iv = 'vZFouPuGJJh3ngsZ'
        self.operatorId = '000000018'
        self.operatorSecret = 'r7275sxnAoIFK0Ct'
        self.seq = '0001'
        self.base_url = 'http://www.sunriseyny.cn:8081/ems-share-api/'

        self.mysql_conn = pymysql.connect(host='192.168.10.11', user='bnse', passwd='123456', port=3316,
                                          db='bnse_originaldata',
                                          charset='utf8')
        self.cursor = self.mysql_conn.cursor()
        self.mysql_conn_ems = pymysql.connect(host='192.168.10.12', user='bnse', passwd='123456', port=3306,
                                              db='bnse_ems_jimo2',
                                              charset='utf8')
        self.cursor_ems = self.mysql_conn_ems.cursor()
        # self.ids = {HSHL201212024015, HSHL201212024019, HSHL201212024020, HSHL201212024021, }
        # self.signs = {1000,1001,1002,1003, }

    def __pad(self, text):
        """填充方式，加密内容必须为16字节的倍数，若不足则使用self.iv进行填充"""
        text_length = len(text)
        amount_to_pad = AES.block_size - (text_length % AES.block_size)
        if amount_to_pad == 0:
            amount_to_pad = AES.block_size
        pad = chr(amount_to_pad)
        return text + pad * amount_to_pad

    def get_md5(self, str):
        enc_res = hmac.new(self.sign_key, str.encode('utf-8'), hashlib.md5).hexdigest()
        # print(enc_res)
        return enc_res

    def AES_Encrypt(self, key, data):
        data = self.__pad(str(data).replace(' ', '').replace('\'', '\"'))
        # print(data)
        # 字符串补位
        cipher = AES.new(key.encode('utf8'), AES.MODE_CBC, self.data_secret_iv.encode('utf8'))
        encryptedbytes = cipher.encrypt(data.encode('utf8'))
        # 加密后得到的是bytes类型的数据，使用Base64进行编码,返回byte字符串
        encodestrs = base64.b64encode(encryptedbytes)
        # 对byte字符串按utf-8进行解码
        enctext = encodestrs.decode('utf8')
        return enctext

    def AES_Decrypt(self, key, data):
        data = data.encode('utf8')
        encodebytes = base64.decodebytes(data)
        # 将加密数据转换位bytes类型数据
        cipher = AES.new(key.encode('utf8'), AES.MODE_CBC, self.data_secret_iv.encode('utf8'))
        text_decrypted = cipher.decrypt(encodebytes)
        # 去补位
        text_decrypted = self.unpad(text_decrypted)
        text_decrypted = text_decrypted.decode('utf8')
        # print(text_decrypted)
        return text_decrypted

    def get_post_data(self, data_before, timeStamp):
        data = self.AES_Encrypt(self.data_secret, data_before)
        strs = self.operatorId + data + timeStamp + self.seq
        # print(strs)
        sig = self.get_md5(strs).upper()
        params = {'operatorId': self.operatorId, 'data': data, 'sig': sig, 'seq': self.seq, 'timeStamp': timeStamp,
                  'data_before': data_before}
        # print(params)
        return params

    def analyze_data(self, res):
        # print(res)
        if not res['data']:
            return None
        sigStr = res['operatorId'] + res['data'] + res['msg'] + str(res['ret'])
        check_sig = self.get_md5(sigStr)
        if res['sig'].lower() == check_sig:
            decrypt = self.AES_Decrypt(self.data_secret, res['data'])
            get_data = json.loads(decrypt)
            # if type(get_data)==dict:
            #     print(get_data['operatorId'])
            # if type(get_data) == list:
            #     print(get_data[0])
            #     print(get_data[1])
            #     print(get_data[2])
            #     print(get_data[99])

            # print(get_data['root'])
            return get_data
        else:
            return None

    def get_token(self):
        url = self.base_url + "queryToken"
        timeStamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_before = {"operatorId": self.operatorId, "operatorSecret": self.operatorSecret}
        # print(str)
        headers = {
            'Content-Type': 'application/json;charset=utf-8'
        }
        params = self.get_post_data(data_before, timeStamp)
        response = requests.request("POST", url, headers=headers, params=params)
        res = json.loads(response.text)
        # print(res)
        get_data = self.analyze_data(res)
        if get_data:
            now = time.time()
            access_token = get_data['accessToken']
            self.redis_conn.set('access_token', access_token)
            self.redis_conn.set('expires_in', get_data['tokenAvailableTime'])
            self.redis_conn.set('token_set_time', now)
            return access_token
        else:
            return None

    def get_numbers(self, url, data):
        access_token = self.redis_conn.get('access_token')
        expires_in = self.redis_conn.get('expires_in')
        token_set_time = self.redis_conn.get('token_set_time')
        timeStamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        url = self.base_url + url
        data_before = data
        if not access_token or not expires_in or not token_set_time:
            access_token = self.get_token()
        else:
            access_token = access_token.decode('utf8')
            expires_in = float(expires_in.decode('utf8'))
            token_set_time = float(token_set_time.decode('utf8'))
            now = time.time()
            if now - token_set_time >= expires_in:
                access_token = self.get_token()
                if not access_token:
                    return 1001
        # access_token = self.get_token()
        headers = {
            'token': access_token,
            'Content-Type': 'application/json;charset=utf-8'
        }
        params = self.get_post_data(data_before, timeStamp)
        response = requests.request("POST", url, headers=headers, params=params)
        res = json.loads(response.text)
        get_data = self.analyze_data(res)
        # print(get_data[0])
        return get_data

    def get_sign(self, BuildingSign, signFrom):
        sql = f"select sign,signFrom from meter where signFrom is not null and BuildingSign=BuildingSign"
        self.cursor_ems.execute(sql)
        data_list = self.cursor_ems.fetchall()
        for i in data_list:
            if (i[1]) == signFrom:
                print(i[0], i[1])
                return i[0]
        return None
        index = data_list.index([tup for tup in data_list if tup[0] == '1005'])

    def updateDatas(self, BuildingSign, datetime_now, datetime_end):
        datetime_last = datetime.datetime(1900, 1, 1, 0, 0, 0)
        sql = f"select sign,signFrom from meter where signFrom is not null and BuildingSign='" + BuildingSign + "' order by sign asc"
        self.cursor_ems.execute(sql)
        data_list = self.cursor_ems.fetchall()
        list_z = []
        table_name = None
        while datetime_now < datetime_end:
            start_time = datetime_now.strftime('%Y%m%d0000')
            start_time_yearmonth = datetime_now.strftime('%Y%m')
            start_time_yearmonthday = datetime_now.strftime('%Y%m%d')
            end_time = (datetime_now + datetime.timedelta(days=1)).strftime('%Y%m%d0000')
            end_time_last = datetime_last.strftime('%Y%m')
            # 不同月份
            if end_time_last != start_time_yearmonth:
                table_name = datetime_now.strftime('%Y%m') + '_recorddata_' + BuildingSign
                try:
                    sql_check = "select count(*) from {}".format(table_name)
                    self.cursor_ems.execute(sql_check)
                except Exception as error:
                    if error.args[0] == 1146:
                        self.cursor.execute(
                            "CREATE TABLE if not exists `{}` (`sign` varchar(50) NOT NULL,`funcid` int(11) NOT NULL,`receivetime` "
                            "datetime NOT NULL,`data` double NOT NULL,PRIMARY KEY (`sign`,`funcid`,`receivetime`),"
                            "KEY `index_intervaldata` (`sign`,`funcid`,`receivetime`) USING BTREE) ENGINE=MyISAM DEFAULT "
                            "CHARSET=utf8;".format(table_name))
            counts = 0
            for i in data_list:
                data = self.get_numbers('queryHistoryCumulantInfo',
                                        {"pointId": i[1], "startTime": start_time, "endTime": end_time,
                                         "type": "0", "pointType": 1})
                if data:
                    counts += len(data)
                    data1 = sorted(data, key=lambda i: i['dataTime'])
                    # for i in data1:
                    #     print(i)
                    list_z = []
                    for ii in data1:
                        list_z.append((i[0], 0, ii['bm'], ii['dataTime']))

                    sql = "REPLACE INTO `{}`(`sign`, `funcid`, `data`,`receivetime`) VALUES (%s,%s,%s,%s);".format(
                        table_name)
                    try:
                        self.cursor.executemany(sql, list_z)
                    except Exception as error:
                        # self.log.error(error)
                        self.mysql_conn.rollback()
                        print('数据接收失败:' + error)
                        break
            self.mysql_conn.commit()
            # self.log.info(table_name + '写入成功')
            print('数据接收成功:' + start_time_yearmonthday + ',' + str(counts) + '条记录')
            datetime_last = datetime_now
            datetime_now = datetime_now + datetime.timedelta(days=1)
        print('数据接收结束')

    def updateLastDatas(self, buildingSign):
        sql = f"select sign,signFrom from meter where signFrom is not null and BuildingSign='" + buildingSign + "' order by sign asc"
        self.cursor_ems.execute(sql)
        data_list = self.cursor_ems.fetchall()
        list_z = []
        year_month_now = datetime.datetime.now().strftime('%Y%m')
        table_name = year_month_now + '_recorddata_' + buildingSign
        for i in data_list:
            data = self.get_numbers('queryLastHistoryCumulantInfo', {"pointId": i[1], "pointType": 1})
            if data:
                data_time = datetime.datetime.strptime(data['dataTime'], '%Y-%m-%d %H:%M:%S')
                year_month = data_time.strftime('%Y%m')
                if year_month != year_month_now:
                    continue
                list_z.append((i[0], 0, data['bm'], data['dataTime']))
        sql = "REPLACE INTO `{}`(`sign`, `funcid`, `data`,`receivetime`) VALUES (%s,%s,%s,%s);".format(
            table_name)
        try:
            sql_check = "select count(*) from {}".format(table_name)
            self.cursor.execute(sql_check)
        except Exception as error:
            if error.args[0] == 1146:
                self.cursor.execute(
                    "CREATE TABLE if not exists `{}` (`sign` varchar(50) NOT NULL,`funcid` int(11) NOT NULL,`receivetime` "
                    "datetime NOT NULL,`data` double NOT NULL,PRIMARY KEY (`sign`,`funcid`,`receivetime`),"
                    "KEY `index_intervaldata` (`sign`,`funcid`,`receivetime`) USING BTREE) ENGINE=MyISAM DEFAULT "
                    "CHARSET=utf8;".format(table_name))
        if list_z != []:
            try:
                counts = self.cursor.executemany(sql, list_z)
            except Exception as error:
                # self.log.error(error)
                self.mysql_conn.rollback()
                print(
                    datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S') + '数据接收失败:' + error)
                # break
            else:
                self.mysql_conn.commit()
            # self.log.info(table_name + '写入成功')
        print(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S') + '数据接收成功[' + str(
            counts) + ']')


def encrypt_by_rsa(data, key_type="pbu_key"):
    if key_type == "pbu_key":
        with open(Path(Path().absolute(), 'pbu_key.txt'), "r") as file_pub:
            key = file_pub.read()
    else:
        with open(Path(Path().absolute(), 'pri_key.txt'), "r") as file_pri:
            key = file_pri.read()
    rsa_key = RSA.importKey(key)
    cipher = PKCS1_v1_5.new(rsa_key)
    text = cipher.encrypt(data.encode("utf-8"))
    return base64.b64encode(text).decode("utf-8")


def decrypt_by_rsa(data, key_type="pri_key"):
    if key_type == "pbu_key":
        with open(Path(Path().absolute(), 'pbu_key.txt'), "r") as file_pub:
            key = file_pub.read()
    else:
        with open(Path(Path().absolute(), 'pri_key.txt'), "r") as file_pri:
            key = file_pri.read()
    rsa_key = RSA.importKey(key)
    cipher = PKCS1_v1_5.new(rsa_key)
    text = cipher.decrypt(base64.b64decode(data), None)
    return text.decode("utf-8").replace("'", '"')


def get_connect(**kwargs):
    user = kwargs.get('username')
    password = kwargs.get('password')
    host = kwargs.get('host')
    port = kwargs.get('port')
    database = kwargs.get('database')
    connect = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(user, password, host, port, database))
    return connect


def check_password(request_pwd, database_pwd):
    pwd1 = decrypt_by_rsa(request_pwd)
    pwd2 = decrypt_by_rsa(database_pwd)
    # print(pwd1,pwd2,pwd1 == pwd2)
    return pwd1 == pwd2


def generate_fake_data(n, rules):
    data_list = []
    ranges = rules.range
    null = rules.null
    zero = rules.zero
    floats = rules.float
    negative = rules.negative
    growth = rules.growth
    if len(ranges) == 1:
        ranges = [ranges[0], ranges[0] + 10]
    for i in range(n):
        if null and random.randrange(0, 100) < 10:
            data_list.append(np.nan)
        else:
            if zero and 0 in ranges and random.randrange(0, 100) < 20:
                data_list.append(0)
            else:
                value = random.uniform(min(ranges), max(ranges))
                if negative:
                    value *= -1
                if floats > 0:
                    value = round(value, floats)
                data_list.append(value)
    if growth and not null:
        data_list.sort()
    elif not growth and not null:
        data_list.sort(reverse=True)
    return data_list

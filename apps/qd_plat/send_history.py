# -*- coding: UTF-8 -*-
"""
青岛公共机构监测平台上传程序
"""
import base64
import json
import os
import sys
import time
import schedule
from loguru import logger
import pymysql
import requests
import datetime
from Crypto.Cipher import AES


def pad_data(s):
    # 字符串补位
    BLOCK_SIZE = 16  # Bytes
    return s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)


def encrypt(auth, text):
    """
    AES的ECB模式加密方法
    :param auth: 密钥
    :param text:被加密字符串（明文）
    :return:密文
    """
    key = auth.encode('utf8')
    data = pad_data(text)
    cipher = AES.new(key, AES.MODE_ECB)
    # 加密后得到的是bytes类型的数据，使用Base64进行编码,返回byte字符串
    result = cipher.encrypt(data.encode())
    encode_str = base64.b64encode(result)
    enc_text = encode_str.decode('utf8')
    return enc_text


# 基本配置类
class SendData(object):

    def __init__(self):
        self.mysql_conn = pymysql.connect(host='192.168.10.12', user='bnse', passwd='123456', port=3306,
                                          db='bnse_energydata',
                                          charset='utf8', cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.mysql_conn.cursor()
        self.headers = {"Content-Type": "application/json"}
        self.mode = AES.MODE_ECB  # 操作模式选择ECB
        self.auth = {}
        self.timestamp = datetime.datetime.now().timestamp()
        self.energy_type_list = [{"tab": "e1", "list": [{"id": "1", "code": "01000"}, {"id": "13", "code": "01A00"},
                                                        {"id": "11", "code": "01B00"},
                                                        {"id": "12", "code": "01C00"}, {"id": "14", "code": "01D00"}]},
                                 {"tab": "e11", "list": [{"id": "2", "code": "02000"}]},
                                 {"tab": "e13", "list": [{"id": "6", "code": "03000"}]},
                                 {"tab": "e3", "list": [{"id": "3", "code": "04000"}]}]
        self.send_list = []

    def post_request(self, url, data):
        response = requests.request("POST", url, data=data, headers=self.headers)
        return response

    def get_auth(self, building_id):
        url = 'http://121.36.56.35:7300/api/upload/login'
        data = json.dumps({
            "projectNumber": building_id['code']
        })
        response = self.post_request(url, data)
        try:
            res = json.loads(response.text)
        except Exception as error:
            logger.error('访问接口失败：' + str(error))
            return False

        if res['code'] == 200:
            # self.timestamp = datetime.datetime.now().timestamp()
            self.auth[building_id['code']] = {"timestamp": datetime.datetime.now().timestamp(), "auth": res['data']}
            logger.info('秘钥获取成功：{}'.format(res['data']))
            return True
        else:
            logger.error('秘钥获取失败：' + res['msg'])
            return False

    def check_auth(self, building_id):
        timestamp = datetime.datetime.now().timestamp()
        if not self.auth.get(building_id['code']) or timestamp - self.auth[building_id['code']]["timestamp"] >= 14 * 60:
            if not self.get_auth(building_id):
                return None
        auth = self.auth[building_id["code"]]["auth"]
        return auth

    def get_hour_data(self, building_id):
        data = []
        data_time = (datetime.datetime.now() + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H:00:00")
        for x in self.energy_type_list:
            table_name = datetime.datetime.now().strftime("%Y%m") + '_energydata_' + str(building_id['id']) + '_' + x[
                'tab'] + '_t1'
            for i in x['list']:
                sql = 'SELECT `timefrom`,`data`FROM {} WHERE `timefrom`="{}" AND `energyid`={}'
                try:
                    self.cursor.execute(sql.format(table_name, data_time, i['id']))
                except pymysql.err.ProgrammingError:
                    continue
                spl_data = self.cursor.fetchone()
                if spl_data:
                    data.append(
                        {"projectNumber": building_id['code'], "subcode": i['code'], "energyVal": spl_data['data'],
                         "energyTime": spl_data['timefrom'].strftime("%Y-%m-%d %H:00:00")})
        logger.info('数据准备完毕，数据时间：{}'.format(data_time))
        data = str(data).replace('\'', '\"')
        return data, data_time

    def check_building_id(self, building_id):
        if isinstance(building_id, list):
            for id in building_id:
                auth = self.check_auth(id)
                if not auth:
                    return
                data, data_time = self.get_hour_data(id)
                self.send_data(id, data, auth, data_time)
        else:
            auth = self.check_auth(building_id)
            if not auth:
                return
            data, data_time = self.get_hour_data(building_id)
            self.send_data(building_id, data, auth, data_time)

    def send_data(self, building_id, data, auth, data_time, type=0):
        e_data = encrypt(auth, data)
        full_data = json.dumps({"projectNumber": building_id['code'], "data": e_data})
        url = 'http://121.36.56.35:7300/api/upload/uploadSubentryEnergy'
        res = json.loads(self.post_request(url, full_data).text)
        if res['code'] == 200:
            logger.info(f'{building_id["code"]}数据发送成功')
            if type == 1:
                obj = filter(lambda item: item['data_time'] == data_time, self.send_list)
                self.send_list.remove(obj)
        else:
            logger.error(
                f'{building_id["code"]}数据发送失败：' + res['msg'] + '\n 本时间:' + data_time + '\n 数据：' + data)
            if type == 1:
                self.send_list.append({'building_id': building_id, 'data': data, 'auth': auth, 'data_time': data_time})

    def send_history(self, building_id, default_time):
        now = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        for y in self.energy_type_list:
            tab_sql = 'select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA="bnse_energydata" and left(TABLE_NAME,6)+0>= {} and TABLE_NAME LIKE "%{}_{}_t1%"'
            # tab_sql = 'select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA="bnse_energydata" and TABLE_NAME LIKE "%{}_{}_t1%"'
            self.cursor.execute(tab_sql.format(default_time, building_id['id'], y['tab']))
            tab_list = self.cursor.fetchall()
            for tab in tab_list:
                sql_one = 'SELECT `timefrom`,`data` FROM {} WHERE `energyid` IN (1,2,5,6) ORDER BY `timefrom` LIMIT 1'
                self.cursor.execute(sql_one.format(tab['TABLE_NAME']))
                tab_one = self.cursor.fetchone()
                if not tab_one:
                    continue
                first_day = tab_one['timefrom'].day
                if tab_one['timefrom'].month in [1, 3, 5, 7, 8, 10, 12]:
                    end_day = 32
                elif tab_one['timefrom'].month == 2:
                    if tab_one['timefrom'].year % 4 == 0:
                        end_day = 30
                    else:
                        end_day = 29
                elif tab_one['timefrom'].month in [4, 6, 9, 11]:
                    end_day = 31
                else:
                    continue
                for x in range(first_day, end_day):
                    data = []
                    start_time = tab_one['timefrom'].strftime('%Y-%m-{} 00:00:00'.format(x))
                    if start_time == now:
                        return
                    end_time = tab_one['timefrom'].strftime('%Y-%m-{} 23:59:59'.format(x))
                    for i in y['list']:
                        sql_day = 'SELECT `timefrom`,`data` FROM {} WHERE `timefrom` BETWEEN "{}" AND "{}" AND `energyid` ={} ORDER BY `timefrom`,`energyid`'
                        self.cursor.execute(sql_day.format(tab['TABLE_NAME'], start_time, end_time, i['id']))
                        day_data = self.cursor.fetchall()
                        for data_one in day_data:
                            data.append(
                                {"projectNumber": building_id['code'], "subcode": i['code'],
                                 "energyVal": data_one['data'],
                                 "energyTime": data_one['timefrom'].strftime("%Y-%m-%d %H:00:00")})
                    auth = self.check_auth(building_id)
                    if not auth:
                        logger.error('权限获取失败')
                        return
                    data = str(data).replace('\'', '\"')
                    logger.info('数据准备完毕，数据时间：{}至{}'.format(start_time, end_time))
                    self.send_data(building_id, data, auth, start_time)


if __name__ == '__main__':
    try:
        if not os.path.exists('./logs'):
            os.mkdir('logs')
        logger.add("./logs/qd-plat-info-{}.log".format(datetime.datetime.now().strftime("%Y-%m-%d")),
                   format="写入时间：{time} 报警等级：{level} 内容：{message}", filter="",
                   level="INFO")
        logger.add("./logs/qd-plat-error-{}.log".format(datetime.datetime.now().date()),
                   format="写入时间：{time} 报警等级：{level} 内容：{message}", filter="",
                   level="ERROR")
        logger.info('启动程序')
        sd = SendData()
        with open('send_list.json', 'r') as f:
            config = json.loads(f.read())
            send_list = config['send_list']
            default_time = config['default_time']
        for building in send_list:
            sd.send_history(building, default_time)


    except Exception as error:
        logger.error(error)
        # 如有异常，尝试再次执行
        print('运行异常，300秒后尝试重启程序')
        time.sleep(300)
        python = sys.executable
        os.execl(python, python, *sys.argv)

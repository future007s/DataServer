"""
海尚海数据接收
"""

from fastapi import FastAPI, HTTPException
import logging
import settings
from utils.utils import BaseFunction
from api import log

logger = log.logger
is_debug = settings.debug
if is_debug:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.WARNING)

app = FastAPI(debug=is_debug)
bf = BaseFunction()


@app.post('/QueryMeterInfo')
def query_meter_info():
    id_list = []
    name_list = []
    type_list = []
    for i in range(99):
        page = i + 1
        get_data = bf.get_numbers('queryMeterInfo', {'pageIndex': page, 'pageLimit': 99})
        if len(get_data['root']) == 0:
            bf.save_csv({'id': id_list, 'name': name_list, 'type': type_list})
            return HTTPException(status_code=200)
        for equ in get_data['root']:
            id = equ['pointId']
            type = equ['energyType']
            name = equ['name']
            id_list.append(id)
            name_list.append(name)
            type_list.append(type)
    bf.save_csv({'id': id_list, 'name': name_list, 'type': type_list})
    return HTTPException(status_code=200)


@app.post('/QueryHistoryCumulantInfo')
def query_history_cumulant_info():
    data = {'pointId': 'HSHL201212006023', 'startTime': '20210731', 'endTime': '20210801', 'pointType': 2,
            'type': '0'}
    get_data = bf.get_numbers('queryHistoryCumulantInfo', data)
    # print(get_data)
    if not get_data:
        return HTTPException(status_code=200)
    for equ in get_data:
        value = equ['bm']
        dataTime = equ['dataTime']
        id = equ['pointId']
        print(value, id, dataTime)
    return HTTPException(status_code=200)


@app.post('/QueryLastHistoryCumulantInfo')
def query_last_history_cumulant_info():
    data = {'pointId': 'HSHL201212006023', 'pointType': 2}
    get_data = bf.get_numbers('queryLastHistoryCumulantInfo', data)
    value = get_data['bm']
    dataTime = get_data['dataTime']
    id = get_data['pointId']
    print(value, id, dataTime)
    return HTTPException(status_code=200)

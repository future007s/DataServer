import subprocess
from datetime import datetime
from api.log import logger
import settings
import asyncio



def control_web_service(start_or_stop:str):
    cmd = f'docker container {start_or_stop} new_energy_server_web_1'
    try:
        cmd_result = subprocess.run(cmd, shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
    except subprocess.TimeoutExpired as err:
        return_code = 1
        fail_msg = f'命令<{cmd}>超时<error:{err}>'
    else:
        return_code = cmd_result.returncode
        if return_code == 0:
            return 0, cmd_result.stdout.decode()
        fail_msg = cmd_result.stderr.decode()
    logger.error(f'{start_or_stop} new_energy_server_web_1 失败,原因:{fail_msg}')
    return 1, fail_msg


def get_now_suffix():
    now = datetime.now()
    return f'{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}_{now.second}_{now.microsecond}'


async def gather_in_bulk(task_list:list):
    """
    控制并发执行任务的最大个数
    """
    start = 0
    total = len(task_list)
    while (stop:=start+settings.worker_amt)<=total:
        await asyncio.gather(*task_list[start:stop])
        start = stop
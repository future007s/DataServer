import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import settings

logger = logging.getLogger("data_reciever")
stream_handler = logging.StreamHandler()
fmt = '[%(asctime)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s'
formatter = logging.Formatter(fmt)
std_formatter = logging.Formatter(fmt)
stream_handler.setFormatter(std_formatter)
logger.addHandler(stream_handler)
stream_handler.setLevel(logging.DEBUG)
# 自动归档日志，最大容量100M，保留10个
rotate_file_handler_error = RotatingFileHandler(
    Path(Path().absolute(), 'logs/error.log'), maxBytes=10 * 1024 * 1024, backupCount=10)
rotate_file_handler_error.setFormatter(formatter)
rotate_file_handler_error.setLevel(logging.ERROR)
logger.addHandler(rotate_file_handler_error)

rotate_file_handler_info = RotatingFileHandler(
    Path(Path().absolute(), 'logs/info.log'), maxBytes=10 * 1024 * 1024, backupCount=10)
rotate_file_handler_info.setFormatter(formatter)
rotate_file_handler_info.setLevel(logging.INFO)
logger.addHandler(rotate_file_handler_info)

rotate_file_handler_debug = RotatingFileHandler(
    Path(Path().absolute(), 'logs/debug.log'), maxBytes=100 * 1024 * 1024, backupCount=10)
rotate_file_handler_debug.setFormatter(formatter)
rotate_file_handler_debug.setLevel(logging.DEBUG)
logger.addHandler(rotate_file_handler_debug)

is_debug = settings.debug
if is_debug:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.WARNING)

import os
import sys
import logging
import logging.config
import colorlog

from logging.handlers import RotatingFileHandler

# 终端输出日志颜色配置
log_colors_config = {
    'DEBUG': 'cyan',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'purple'
}

default_formats = {
    # 日志输出格式
    "file": '[%(asctime)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s',
    # 终端输出格式
    "console": '%(log_color)s[%(asctime)s] [%(name)s] [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s'
}


class InfoFilter(logging.Filter):
    """
    过略日志记录级别，只保留INFO记录
    """

    def filter(self, record):
        if record.levelno == 20:
            return True
        return False


class WarningFilter(logging.Filter):
    """
    过略日志记录级别，只保留Warning记录
    """

    def filter(self, record):
        if record.levelno == 30:
            return True
        return False


class ErrorFilter(logging.Filter):
    """
    过滤日志记录级别，只保留ERROR记录
    """

    def filter(self, record):
        if record.levelno == 40:
            return True
        return False


class Log:
    """
    配置日志：分为main日志和各个building日志
    building日志分为info、warning、error日志等，目前程序中用到info和error，warning可根据需要添加。
    也可根据需求按照info等的配置方式自行添加。
    """
    def make_dir(self, file):
        base_dir = os.path.dirname(__file__)
        path = base_dir + f"\{file}"
        folder = os.path.exists(path)
        if not folder:
            os.makedirs(path)

    def add_StreamHandler(self):
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = colorlog.ColoredFormatter(default_formats["console"], log_colors=log_colors_config)
        console_handler.formatter = formatter
        self.logger.addHandler(console_handler)

    def add_error_FileHandler(self, building_id, file, base_dir, formatter):
        filepath_error = base_dir + f'\{file}' + f'\{building_id}_error.log'
        file_handler_error = RotatingFileHandler(filename=filepath_error, maxBytes=500 * 1024 * 1024, backupCount=5,
                                                 encoding="utf-8")
        file_handler_error.setFormatter(formatter)
        file_handler_error.setLevel(level=logging.ERROR)
        file_handler_error.addFilter(ErrorFilter())
        self.logger.addHandler(file_handler_error)

    def add_info_FileHandler(self, building_id, file, base_dir, formatter):
        filepath_info = base_dir + f'\{file}' + f'\{building_id}_info.log'
        file_handler_info = RotatingFileHandler(filename=filepath_info, maxBytes=500 * 1024 * 1024, backupCount=5,
                                                encoding="utf-8")
        file_handler_info.setFormatter(formatter)
        file_handler_info.setLevel(level=logging.INFO)
        file_handler_info.addFilter(InfoFilter())
        self.logger.addHandler(file_handler_info)

    def add_warning_FileHandler(self, building_id, file, base_dir, formatter):
        filepath_warning = base_dir + f'\{file}' + f'\{building_id}_warning.log'
        file_handler_warning = RotatingFileHandler(filename=filepath_warning, maxBytes=500 * 1024 * 1024, backupCount=5,
                                                encoding="utf-8")
        file_handler_warning.setFormatter(formatter)
        file_handler_warning.setLevel(level=logging.WARNING)
        file_handler_warning.addFilter(WarningFilter())
        self.logger.addHandler(file_handler_warning)

    def get_logger(self, name, file=None):
        self.logger = logging.getLogger(name=name)
        self.logger.setLevel(level=logging.DEBUG)
        if name == "main":
            self.make_dir(file)
            base_dir = os.path.dirname(__file__)
            formatter = logging.Formatter(default_formats["file"])
            filepath_main = base_dir + f'\{file}' + '\main.log'
            file_handler_main = RotatingFileHandler(filename=filepath_main, maxBytes=500 * 1024 * 1024, backupCount=5,
                                                    encoding="utf-8")
            file_handler_main.setFormatter(formatter)
            file_handler_main.setLevel(level=logging.DEBUG)
            self.logger.addHandler(file_handler_main)
            self.add_StreamHandler()
            return self.logger
        self.add_StreamHandler()
        base_dir = os.path.dirname(__file__)
        formatter = logging.Formatter(default_formats["file"])
        self.add_info_FileHandler(name, file, base_dir, formatter)
        self.add_error_FileHandler(name, file, base_dir, formatter)
        self.add_warning_FileHandler(name, file, base_dir, formatter)
        return self.logger

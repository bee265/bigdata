#!/usr/bin/python3
# encoding: utf-8

"""
@author: fangwh
@contact: fangwh265@hotmail.com
@project: bigdata
@file: logger.py
@create_time: 2018/11/29 22:53

"""
import logging
import logging.handlers

class logger:
    __file = 'log.log'#日志文件名称
    __handler = False
    __fmt = '%(asctime)s - %(filename)s:[line:%(lineno)s] - %(name)s - %(message)s'#输出格式

    def __init__(self):
        logging.basicConfig(filename=self.__file, filemode='a+', format=self.__fmt)
        # self.__handler = logging.handlers.RotatingFileHandler(self.__file, maxBytes=1024*1024, backupCount=5)
        self.__handler = logging.StreamHandler()
        self.__handler.setLevel(logging.INFO)

        #设置格式
        formatter = logging.Formatter(self.__fmt)
        self.__handler.setFormatter(formatter)
        return

    #获取实例
    def getInstance(self, strname):
        logger = logging.getLogger(strname)
        logger.addHandler(self.__handler)
        logger.setLevel(logging.DEBUG)
        return logger

if __name__ == '__main__':
    testlog = logger().getInstance("test")
    testlog1 = logger().getInstance("tes222t")
    testlog.info("info log")
    testlog.debug("debug log")
    testlog.warning("waring log")
    testlog1.info("info log")
    testlog1.debug("debug log")
    testlog1.warning("waring log")

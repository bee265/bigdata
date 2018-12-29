#!/usr/bin/python3
# encoding: utf-8

"""
@author: fangwh
@contact: fangwh265@hotmail.com
@project: bigdata
@file: test.py
@create_time: 2018/11/25 21:02

"""
import configparser
import logging
def readconfig():

    cf = configparser.ConfigParser()
    path = r''
    cf.read('..\config\src_config.conf')
    print(cf.sections())
    print(cf.get("email","mail_password"))
    print(cf.get("db","db_port"))

if __name__ == '__main__':

    import logger

    x = logger.logger()

    x.critical("这是一个 critical 级别的问题！")
    x.error("这是一个 error 级别的问题！")
    x.warning("这是一个 warning 级别的问题！")
    x.info("这是一个 info 级别的问题！")
    x.debug("这是一个 debug 级别的问题！")

    x.log(50, "这是一个 critical 级别的问题的另一种写法！")
    x.log(40, "这是一个 error 级别的问题的另一种写法！")
    x.log(30, "这是一个 warning 级别的问题的另一种写法！")
    x.log(20, "这是一个 info 级别的问题的另一种写法！")
    x.log(10, "这是一个 debug 级别的问题的另一种写法！")

    x.log(51, "这是一个 Level 51 级别的问题！")
    x.log(11, "这是一个 Level 11 级别的问题！")
    x.log(9, "这条日志等级低于 debug，不会被打印")
    x.log(0, "这条日志同样不会被打印")

    """
    运行结果：
    2018-10-12 00:18:06,562 - demo - CRITICAL - 这是一个 critical 级别的问题！
    2018-10-12 00:18:06,562 - demo - ERROR - 这是一个 error 级别的问题！
    2018-10-12 00:18:06,562 - demo - WARNING - 这是一个 warning 级别的问题！
    2018-10-12 00:18:06,562 - demo - INFO - 这是一个 info 级别的问题！
    2018-10-12 00:18:06,562 - demo - DEBUG - 这是一个 debug 级别的问题！
    2018-10-12 00:18:06,562 - demo - CRITICAL - 这是一个 critical 级别的问题的另一种写法！
    2018-10-12 00:18:06,562 - demo - ERROR - 这是一个 error 级别的问题的另一种写法！
    2018-10-12 00:18:06,562 - demo - WARNING - 这是一个 warning 级别的问题的另一种写法！
    2018-10-12 00:18:06,562 - demo - INFO - 这是一个 info 级别的问题的另一种写法！
    2018-10-12 00:18:06,562 - demo - DEBUG - 这是一个 debug 级别的问题的另一种写法！
    2018-10-12 00:18:06,562 - demo - Level 51 - 这是一个 Level 51 级别的问题！
    2018-10-12 00:18:06,562 - demo - Level 11 - 这是一个 Level 11 级别的问题！
    """


#!/usr/bin/python3
# -*- coding: utf-8 -*-

# @Time    : 2018/11/30 9:04
# @Author  : fangwh
# @Email   : fangwh265@hotmail.com
# @File    : main.py
import sys
sys.path.append('/home/hdp/etl')
import configparser
from business_sql.src_loaddata import loadfromfile,insert_ods,replace_ods,update_ods
from hadoop_api.mysqlapi import Mysql_client
from utils import logger
srclog = logger.logger().getInstance('main')

# 导入数据表信息,调用输入的两个变量
tb_name = 'tb_mail_cost'
deal_date = '20180102'

# 引入日志模块
# srclog = logger.logger()
# 读入配置文件
config = configparser.ConfigParser()
# 指定配置文件存放位置，此变量非常重要，请必须填写!!!!!!
config_file = 'config/src_config.conf'
config.read(config_file)

# 读取mysql链接信息
my_db_host = config.get("mysqldb","db_host")
my_port = int(config.get("mysqldb","port"))
my_username = config.get("mysqldb","username")
my_password = config.get("mysqldb","password")
my_database = config.get("mysqldb","database")

# 指定输出日志目录，用于记录hive脚本执行信息
log_dir = config.get("hivefromdata_src","log_file_directory")
# 读取数据文件存放目录
data_dir = config.get("hivefromdata_src","data_file_directory")

# 读取导入的数据库名称
src_db_name = config.get("hivefromdata_src","db_name")
tar_db_name = config.get("hivefromdata_ods","db_name")

# 读取数据文件命名前缀
mysql = Mysql_client(db_host=my_db_host,port=my_port, username=my_username,password=my_password, \
database=my_database)
v_sql = "SELECT file_name,upp_flag FROM int_info where tb_name = '%s'" %  tb_name
reslist = mysql.ExecQuery(v_sql)
file_name =  reslist[0][0]
update_flag = reslist[0][1]
# 导入数据
srclog.info("启动导入程序")
loaddata = loadfromfile(data_dir, file_name, src_db_name, tb_name, deal_date)
if loaddata == 0:

    # 更新ods库
    if update_flag == 'A':
        # ods，更新方式为insert方式

        insert_ods(src_db_name, tb_name, tar_db_name, tb_name,deal_date)
    elif update_flag == 'R':
        # ods，更新方式为replace方式

        replace_ods(src_db_name, tb_name, tar_db_name, tb_name)
        # 测试ods更新数据
    elif update_flag == 'U':

        update_ods(src_db_name, tb_name, tar_db_name, tb_name)
    else:
        print("%s表更新方式不对！" % tb_name)
else:
    print("%s表数据导入失败！" % tb_name)
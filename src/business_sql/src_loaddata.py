#!/usr/bin/python3
# -*- coding: utf-8 -*-

# @Time    : 2018/11/26 15:56
# @Author  : fangwh
# @Email   : fangwh265@hotmail.com
# @File    : src_loaddata.py


from datetime import datetime,timedelta
import os
from hadoop_api.hiveapi import HiveClient
from hadoop_api.mysqlapi import Mysql_client
import configparser
from utils import logger
# 引入日志模块
srclog = logger.logger().getInstance('srddata')
# 读取配置文件
config = configparser.ConfigParser()
config_file = '/home/hdp/etl/src/config/src_config.conf'
config.read(config_file)

# 读取mysql链接信息
my_db_host = config.get("mysqldb","db_host")
my_port = int(config.get("mysqldb","port"))
my_username = config.get("mysqldb","username")
my_password = config.get("mysqldb","password")
my_database = config.get("mysqldb","database")
# 读取hive链接信息
hive_db_host = config.get("hivedb","db_host")
hive_port = int(config.get("hivedb","port"))
hive_username = config.get("hivedb","username")


def loadfromfile(data_dir, file_name, db_name, tb_name, deal_date):
    """

    :param tb_name:
    :param file_name:
    :param deal_date: 20180102
    :return:
    """

    # zip_file= file_name + deal_date.strftime('%Y%m%d') + '.bz2'
    zip_file = file_name + deal_date + '.txt.bz2'
    if os.path.isfile(os.path.join(data_dir, zip_file)):

        # 调用shell命令解压文件
        os.system('bunzip2 ' + os.path.join(data_dir, zip_file))
        # 调用shell hive -e 执行sql，把数据导入数据库
        file_name = file_name + deal_date + '.txt'
        v_sql = "load data local inpath '%s' overwrite into table %s.%s;" % (os.path.join(data_dir, file_name), db_name, tb_name)
        v_sql = '"' + v_sql + '"'
        srclog.info("开始导入数据到表%s.%s！" % (db_name, tb_name))
        os.system('hive -e ' + v_sql)
        try:
            v_sql = 'select count(1) from  %s.%s' % (db_name, tb_name)
            hcon = HiveClient(hive_db_host,hive_port,hive_username)
            res = hcon.query(v_sql)
            hcon.close()
            srclog.info('插入了%s 行' % res[0][0])
            return 0
        except Exception as tx:
            srclog.error('excepion %s' % (tx.message))
            return 1
    else:
        srclog.error('File is not found！')
        return 1


def insert_ods(src_db_name, src_tb_name, tar_db_name, tar_tb_name,deal_date):
    v_mnoth = deal_date[:6]
    v_day = deal_date[4:6]
    # 调用shell hive -e 执行sql，把数据导入ods库
    v_sql = "insert overwrite table %s.%s partition(month_part = '%s',day_part = '%s') \
        select * from %s.%s" % (tar_db_name, tar_tb_name, v_mnoth, v_day, src_db_name, src_tb_name)
    # v_sql = '"' + v_sql + '"'
    # try:
    #     os.system('hive -e ' + v_sql)
    # except Exception as tx:
    #     print('excepion %s' % (tx.message))

    v_sql_src = 'select count(1) from  %s.%s' % (src_db_name, src_tb_name)
    v_sql_ods = 'select count(1) from  %s.%s' % (tar_db_name, tar_tb_name)

    srclog.info("%s表更新方式是'A'！" % tar_tb_name)
    hcon = HiveClient(hive_db_host,hive_port,hive_username)
    hcon.exec(v_sql)
    res_src = hcon.query(v_sql_src)
    res_ods = hcon.query(v_sql_ods)
    hcon.close()
    if res_src[0][0] == res_ods[0][0]:
        srclog.info("%s 数据导入成功，一共处理了%s条记录" % (tar_tb_name,res_ods[0][0]))
        return 0
    else:
        srclog.info("%s 数据导入不成功，一共处理了%s条记录" % (tar_tb_name,res_ods[0][0]))
        srclog.info("%s 数据导入不成功，源数据有%s条记录" % (tar_tb_name,res_src[0][0]))
        return 1


def update_ods(src_db_name, src_tb_name, tar_db_name, tar_tb_name):

    mysql = Mysql_client(db_host=my_db_host, port=my_port, username=my_username, password=my_password, \
                         database=my_database)
    reslist = mysql.ExecQuery("select tb_name,col_name,is_pk from tb_cols where tb_name = '%s' " % tar_tb_name)
    pk = []
    columns = []
    for i in reslist:
        if i[2]==1:
            pk.append(i[1])
        else:
            columns.append(i[1])

    pkcol = []
    for i in pk:
        pkcol.append('stage.%s = target.%s'% (i,i))
    mergepk = ' and '.join(pkcol)

    updcols = []
    for i in columns:
        updcols.append('%s = stage.%s'% (i,i))
    mergecols_upd =' , '.join(updcols)

    inscols = []
    for i in pk:
        inscols.append('stage.%s' % i)
    for i in columns:
        inscols.append('stage.%s' % i)
    mergecols_ins = ','.join(inscols)

    v_sql = 'merge into %s.%s as target using %s.%s as stage on ( %s ) \
when matched then update set %s \
when not matched then insert values (%s)' % (tar_db_name,tar_tb_name,src_db_name,src_tb_name,mergepk,mergecols_upd,mergecols_ins)
    hcon = HiveClient(hive_db_host, hive_port, hive_username)
    srclog.info("%s表更新方式是'U'！" % tar_tb_name)
    hcon.exec(v_sql)
    hcon.close()
    srclog.info("%s表更新完成'！" % tar_tb_name)
    # print(v_sql)
    # 调用shell hive -e 执行sql，把数据导入ods库
    # v_sql = '"' + v_sql + '"'
    # try:
    #     os.system('hive -e ' + v_sql)
    # except Exception as tx:
    #     print('excepion %s' % (tx.message))

def replace_ods(src_db_name, src_tb_name, tar_db_name, tar_tb_name):
    # 把数据导入ods库
    v_sql_truncate = "truncate table %s.%s" % (tar_db_name, tar_tb_name)
    v_sql_insert = "insert into table %s.%s \
            select * from %s.%s" % (tar_db_name, tar_tb_name, src_db_name, src_tb_name)
    # v_sql = '"' + v_sql + '"'
    try:
    #     os.system('hive -e ' + v_sql)
        hcon = HiveClient(hive_db_host, hive_port, hive_username)
        srclog.info("%s表更新方式是'R'！" % tar_tb_name)
        srclog.info("清理%s表的数据！" % tar_tb_name)
        hcon.exec(v_sql_truncate)
        srclog.info("导入数据到%s表！" % tar_tb_name)
        hcon.exec(v_sql_insert)
        v_sql_src = 'select count(1) from  %s.%s' % (src_db_name, src_tb_name)
        v_sql_ods = 'select count(1) from  %s.%s' % (tar_db_name, tar_tb_name)
        # hcon = HiveClient(hive_db_host,hive_port,hive_username)
        res_src = hcon.query(v_sql_src)
        res_ods = hcon.query(v_sql_ods)
        hcon.close()
        if res_src[0][0] == res_ods[0][0]:
            srclog.info("%s 数据导入成功，一共处理了%s条记录" % (tar_tb_name, res_ods[0][0]))
        else:
            srclog.warning("%s 数据导入不成功，一共处理了%s条记录" % (tar_tb_name, res_ods[0][0]))
            srclog.warning("%s 数据导入不成功，源数据有%s条记录" % (tar_tb_name, res_src[0][0]))
    except Exception as tx:
        srclog.error('excepion %s' % (tx.message))

if __name__ == '__main__':
    try:
        v_sql = 'select count(1) from  %s.%s' % ('src_test', 'tb_mail')
        hcon = HiveClient(hive_db_host,hive_port,hive_username)
        res = hcon.query(v_sql)
        hcon.close()
        srclog.info('插入了%s 行' % res[0][0])
    except Exception as tx:
        srclog.error('excepion %s' % (tx.message))
    # update_ods('src_test', 'tb_mail_cost','ods_test','tb_mail_cost')
    # pass

    # db_name1 = 'src_test'
    # loadfromfile(datadir, db_name1, 'tb_mail_cost', '20180101')
    # sql = 'select count(1) from src_test.tb_mail_cost'
    # result = query(sql)
    # print(result[0][0])
    # sql2 = 'select count(1) from  %s.%s' % (db_name1, 'tb_mail_cost')
    # hcon = HiveClient('hd2', 10000, 'hive', 'default')
    # res = hcon.query(sql2)
    # hcon.close()
    # print('insert %s row' % res[0][0])



    # Hive
    # engine = create_engine('hive://hd2:10000/default')
    # logs = Table('employee', MetaData(bind=engine), autoload=True)
    # print(select([func.count('*')], from_obj=logs).scalar())
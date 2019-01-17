#!/usr/bin/python3
# encoding: utf-8

"""
@author: fangwh
@contact: fangwh265@hotmail.com
@project: bigdata
@file: hiveapi.py
@create_time: 2018/12/4 22:25

"""
from pyhive import hive

class HiveClient(object):

    def __init__(self,hive_db_host,hive_port,hive_username):
        """
        create connection to hive server
        """
        # self.db_host = hive_db_host
        # self.port = hive_port
        # self.username = hive_username
        self.conn = hive.Connection(host=hive_db_host, port=hive_port, username=hive_username)

    def query(self, sql):
        """
        query
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        cursor.close()
        return res

    def exec(self, sql):
        """
        query
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        cursor.close()

    def close(self):
        """
        close connection
        """
        self.conn.close()


if __name__ == '__main__':
    v_sql_ods = 'desc  %s.%s' % ('src_test', 'tb_mail_actsettle')
    hcon = HiveClient('hd2', 10000, 'hive')
    # hcon = HiveClient()
    res_src = hcon.query(v_sql_ods)
    dd = 1
    for i in res_src:
        print("('tb_mail_actsettle','%s',0,%d)," % (i[0],dd))
        dd +=1
    hcon.close()
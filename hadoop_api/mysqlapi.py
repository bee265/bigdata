#!/usr/bin/python3
# -*- coding: utf-8 -*-

# @Time    : 2018/12/4 9:05
# @Author  : fangwh
# @Email   : fangwh265@hotmail.com
# @File    : mysqlapi.py


import pymysql

class Mysql_client(object):
    """
    对pymysql的简单封装
    """
    def __init__(self,db_host,username,password,database,port=3306,charset='utf8'):
        self.db_host = db_host
        self.port = port
        self.username = username
        self.password = password
        self.charset = charset
        self.database = database

    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.database:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymysql.connect(host=self.db_host,user=self.username,password=self.password,database=self.database,port=self.port,charset=self.charset)
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MYSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        #查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()




if __name__ == '__main__':
    mysql = Mysql_client(db_host="hd5",port=3306, username="root",password="AAA111aaa",database='cpdds_etl')
    resList = mysql.ExecQuery("SELECT * FROM tb_cols")
    for inst in resList:
        print(inst)
#!/usr/bin/python3
# encoding: utf-8

"""
@author: fangwh
@contact: fangwh265@hotmail.com
@project: bigdata
@file: sparkapi.py
@create_time: 2018/12/31 22:43

"""
#初始化Spark SQL
#导入Spark SQL
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext,Row
#当不能引入Hive依赖时
from pyspark.sql import SQLContext,Row
conf = SparkConf().setAppName(myapp)
sc = SparkContext(conf=conf)
#创建SQL上下文环境
hc = HiveContext(sc)
#基本查询示例
rows = hc.sql("select * from student").collect()
sc.stop()


#!/usr/bin/python3
# encoding: utf-8

"""
@author: fangwh
@contact: fangwh265@hotmail.com
@project: bigdata
@file: hbaseapi.py
@create_time: 2018/11/10 23:32

"""
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *

# server端地址和端口,web是HMaster也就是thriftServer主机名,9090是thriftServer默认端口
transport = TSocket.TSocket('hd5', 9090)
# 可以设置超时
transport.setTimeout(10000)
# 设置传输方式（TFramedTransport或TBufferedTransport）
trans = TTransport.TBufferedTransport(transport)
# 设置传输协议
protocol = TBinaryProtocol.TBinaryProtocol(trans)
# 确定客户端
client = Hbase.Client(protocol)
# 打开连接
transport.open()

def create_table(table_name,*cf):
    """
    :param table_name: 表名
    :param cf:列族名字可多个
    :return:
    """
    # 定义列族
    col=[]
    for i in cf:
        col.append(ColumnDescriptor(name=i))
    # print(col)
    # 创建表
    client.createTable(table_name, col)


def get_row_cf(table_name,rowkey,colfamily):
    """

    :param table_name: 表名
    :param args: 列族名字
    :return:
    """

    getresult = client.get(table_name, rowkey, colfamily)
    valuelist = []
    for i in getresult:
        valuelist.append(i.value)
    return  valuelist

def get_row_col(table_name,rowkey,columns):
    """

    :param table_name: 表名
    :param columns: 列族名字可多个,用列表传入['cf1:a','cf2:a']
    :return:
    """
    getresult = client.getRowWithColumns(table_name, rowkey, columns)
    for item in getresult:
        result = {}
        result['rowkey']=item.row
        for k,v in item.columns.items():
            result[k]=v.value
    return (result)

def get_row(table_name,rowkey):
    getresult = client.getRow(table_name,rowkey)      # result为一个列表，获取表中指定行在最新时间戳上的数据
    for item in getresult:
        result = {}
        result['rowkey']=item.row
        for k,v in item.columns.items():
            result[k]=v.value
    return result

def scan_start_stop(table_name, start_row, stop_row, columns):
    """

    :param table_name:
    :param start_row: 包含start_row的记录
    :param stop_row: 不包含stop_row的记录，传入的参数stop_row可以等于start_row
    :param columns: 列族名或者字段名 ['info1:a', 'info2:b'] 或者['info', 'info2:b']
    :return:
    """
    stop_row = stop_row[:-1]+chr(ord(stop_row[-1])+1)   # stop_row末位加1
    scannerId = client.scannerOpenWithStop(table_name, start_row, stop_row, columns)
    result=[]
    while True:
        getresult = client.scannerGet(scannerId)  # 根据ScannerID来获取结果
        if not getresult:
            break
        for item in getresult:
            qresult = {}
            qresult['rowkey'] = item.row
            for k, v in item.columns.items():
                qresult[k] = v.value
        result.append(qresult)
    client.scannerClose(scannerId)  # 关闭扫描器
    return result

def insert_row_col(table_name,rowkey,columns):
    mutation = []
    for item in columns:
        mutation.append(Mutation(column=item[0], value=item[1]))
    client.mutateRow(table_name, rowkey, mutation)


def insert_row_col_batch(table_name,batchrows):
    """

    :param table_name:
    :param batchrows: [{'rowkey':'5','info:a':'bee55','info:b':'55'},{'rowkey':'8','info:a':'bee88','info:b':'88'}]
    :return:
    """
    rowMutations =[]

    for rowitem in batchrows:
        cf = []
        for k, v in rowitem.items():
            if k =='rowkey':
                rowkey=v
            else:
                cf.append(Mutation(column=k, value=v))
        rowMutations.append(BatchMutation(rowkey,cf))
    client.mutateRows(table_name, rowMutations)


def drop_table(table_name):
    if (client.isTableEnabled(table_name)):
        client.disableTable(table_name)
    client.deleteTable(table_name)  # 删除表.必须确保表存在,且被禁用


# 建表测试
# colfamily = ['cf','info1','info2']
# create_table('test',*colfamily)

# 查询所有表
# print(client.getTableNames())
# 查询表的描述信息
# print(client.getColumnDescriptors('test'))
# 查询表的regions信息
# print(client.getTableRegions('test'))


# 查询列族和字段
# print(get_row_cf('person_hbase','1','info'))
# print(get_row_col('person_hbase','2',['info:age','info:name','info:area']))
# print(get_row_col('test','2',['info1']))

# 插入或修改（所选字段存在值就覆盖）数据
# insert_row_col('test','3',[['info1:a','bee'],['info2:b','123']])

# 批量插入或修改（所选字段存在值就覆盖）数据
# insert_row_col_batch('test', [{'rowkey':'5','info:a':'bee55','info:b':'55'},{'rowkey':'8','info:a':'bee88','info:b':'88'}])
# 扫描表
# 用start_row和stop_row查询对应的列族或者字段
# print(scan_start_stop('test', '1', '3', ['info1', 'info2:b']))
# print(scan_start_stop('test', '1', '3', ['info1:a', 'info2:b']))
# print(scan_start_stop('qry_cdm_dtl_10y', '35062419390816082X', '35062419390816082X', ['info']))


# 删除表记录
# client.deleteAll('test','2','info1:a')  # 删除指定表指定行与指定列的所有数据
# client.deleteAllTs('test','2','info1:a',timestamp=1513569725685)  # 删除指定表指定行与指定列中，小于等于指定时间戳的所有数据
# client.deleteAllRowTs('test','2',timestamp=1513568619326)   # 删除指定表指定行中，小于等于此时间戳的所有数据
# client.deleteAllRow('test','2')  # 删除整行数据

# 删除表测试，先disable，再delete
# drop_table('test4')

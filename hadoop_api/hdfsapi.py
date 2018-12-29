#!/usr/bin/python3
# -*- coding: utf-8 -*-

# @Time    : 2018/11/13 9:26
# @Author  : fangwh
# @Email   : fangwh265@hotmail.com
# @File    : hdfsapi.py

import pyhdfs
client = pyhdfs.HdfsClient(hosts="hd1,8020",user_name="hdp")

def mkdir(remotepath):
    if not exists(remotepath):
        client.mkdirs(dir)

def get(remotefile, localfile):
    if exists(remotefile):
        client.copy_to_local(remotefile, localfile)


def put(localfile, remotefile):
    if not exists(remotefile):
        client.copy_from_local(localfile, remotefile)


def exists(remotepath):
    return client.exists(remotepath)


def delete(remotefile):
    if exists(remotefile):
        client.delete(remotefile)
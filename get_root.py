#!/usr/bin/python3
# -*- coding: utf-8 -*-

# @Time    : 2018/12/5 14:20
# @Author  : fangwh
# @Email   : fangwh265@hotmail.com
# @File    : get_root.py

import os

def get_root():
    return os.path.dirname( os.path.abspath( __file__ ) )
# coding=utf-8
from __future__ import print_function, division, unicode_literals
import os
import time


def humansize(size):
    """将文件的大小转成带单位的形式
    >>> humansize(1024) == '1 KB'
    True
    >>> humansize(1000) == '1000 B'
    True
    >>> humansize(1024*1024) == '1 M'
    True
    >>> humansize(1024*1024*1024*2) == '2 G'
    True
    """
    units = ['B', 'KB', 'M', 'G', 'T']
    for unit in units:
        if size < 1024:
            break
        size = size // 1024
    return '{} {}'.format(size, unit)

# -*- coding:utf-8 -*-
from hdfs3.core import HDFileSystem

hdfs = HDFileSystem(host='fetch-loadtest-20', port=8020)

def get(remotepath, localpath):
    if exists(remotepath):
        hdfs.get(remotepath, localpath)

def exists(remotepath):
    return hdfs.exists(remotepath)

# 执行主方法
if __name__ == '__main__':

    print "监控HDFS"

    print exists('/user/root')

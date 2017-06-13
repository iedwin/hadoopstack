# -*- coding:utf-8 -*-
from hdfs.client import Client

client = Client("http://fetch-loadtest-20:50070")

# 执行主方法
if __name__ == '__main__':
    print "监控HDFS"

    print client.status("/user/hive/createtable2.sh")

# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 21:07:56 2017
信用报告V2.0社保部分，Hive入口
线下4908个用户耗时757.49000001(s)
@author: yaohe1
"""
import jpype
from jpype import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

if __name__ == '__main__':

    print '==================== 进入程序 ===================='

    """ 数据预处理 """
    # 读取数据
    path = '/home/yaohe1/xybg/shebao/'
    # spark连接数据库获取数据
    conf = SparkConf().setMaster("yarn-client").setAppName("Telecom App")
    conf.set("spark.driver.allowMultipleContexts","true")
    conf.set("spark.dynamicAllocation.enabled","false")


    sc = SparkContext(conf = conf)
    hc = HiveContext(sc)

    print '==================== Hive连接成功 ===================='
    shebao_text = hc.sql("select * from shebao.userinfo limit 1000") # 社保数据
    print '==================== shebao_text has readed ===================='
    shebao_text = shebao_text.toPandas().astype('object')

    # 全量读会内存溢出
    blacklist_email_text = hc.sql("select * from default.black_gray_em_email limit 10") # 黑名单数据email
    print '==================== blacklist_email_text has readed ===================='
    blacklist_email_text = blacklist_email_text.toPandas().astype('object')

    blacklist_mobile_text = hc.sql("select * from default.black_gray_em_mobile limit 1000") # 黑名单数据mobile
    print '==================== blacklist_mobile_text has readed ===================='
    blacklist_mobile_text = blacklist_mobile_text.toPandas().astype('object')

    print '==================== 数据读取成功 ===================='



    jvm_path=jpype.getDefaultJVMPath()
    jpype.startJVM(jvm_path)
    java.lang.System.clearProperty("spark.driver.port")

    sc.stop()


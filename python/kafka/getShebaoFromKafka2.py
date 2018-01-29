# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 11:19:14 2018
使用kafka-Python 1.3.3模块
@author: yaohe1
"""

# -*- coding: utf-8 -*-

import time
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
#from TestDecryptKafkaMsgUtil import TestDecryptKafkaMsgUtil
#tdkmu = TestDecryptKafkaMsgUtil()

##KAFAKA_HOST_SPIDER = '10.141.6.211' # 爬虫kafka测试环境，group.id = honeycomb-billing
KAFAKA_HOST_BI = '10.141.5.22,10.141.5.25,10.141.5.26' # 数据分析kafka测试环境，group.id = honeycomb-xybg
KAFAKA_PORT = 9092

#==============================================================================
# honeycomb-gw-taobao 淘宝
# honeycomb-gw-gjj 公积金
# honeycomb-gw-phone 运营商
# honeycomb-gw-pbccrc 征信
# honeycomb-gw-mfa 信用卡账单
# honeycomb-gw-cob 网银
# honeycomb-gw-baodan 保单
# honeycomb-gw-shebao 社保
# honeycomb-gw-taobaoCore 淘宝（对内）
# honeycomb-gw-phoneCore 运营商（对内）
# honeycomb-gw-dob 借记卡
#==============================================================================

KAFAKA_TOPIC = 'honeycomb-gw-shebao' # gw2是自测，gw是测试环境

class Kafka_producer():
    '''
    生产模块：根据不同的key，区分消息
    '''

    def __init__(self, kafkahost,kafkaport, kafkatopic, key):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.key = key
        self.producer = KafkaProducer(bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
                                        kafka_host=self.kafkaHost,
                                        kafka_port=self.kafkaPort)
                        )

    def sendjsondata(self, params):
        try:
            parmas_message = json.dumps(params)
            producer = self.producer
            producer.send(self.kafkatopic, key=self.key, value=parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print e



class Kafka_consumer():
    '''
    消费模块: 通过不同groupid消费topic里面的消息
    '''

    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.groupid = groupid
        self.key = key
        # setting保值一致
        self.enable_auto_commit=False
        self.auto_commit_interval_ms=1000
        self.fetch_max_bytes=1024*1024*20
        self.max_partition_fetch_bytes=1024*1024*20
#        self.max_poll_records=50
        self.session_timeout_ms=30000
        #   self.key_deserializer='org.apache.kafka.common.serialization.StringDeserializer' # 默认None
        #   self.value_deserializer='org.apache.kafka.common.serialization.StringDeserializer' # 默认None
        self.consumer = KafkaConsumer(self.kafkatopic, group_id = self.groupid,
                                    bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
                                        kafka_host=self.kafkaHost,
                                        kafka_port=self.kafkaPort )
                        )
               
    def consume_data(self):
        try:
            for message in self.consumer:
                yield message
        except KeyboardInterrupt, e:
            print e


xtype = 'consumer'
group = 'xxo-test'
key = 'honeycomb-xybg'

'''
测试consumer和producer
'''

#if xtype == 'producer':
#    # 生产模块
#    producer = Kafka_producer(KAFAKA_HOST_BI, KAFAKA_PORT, KAFAKA_TOPIC, key)
#    print "===========> producer:", producer
#    for _id in range(100):
#       params = '{"msg" : "%s"}' % str(_id)
#       producer.sendjsondata(params)
#       time.sleep(1)

if xtype == 'consumer':
    # 消费模块
    consumer = Kafka_consumer(KAFAKA_HOST_BI, KAFAKA_PORT, KAFAKA_TOPIC, group)
    print "===========> consumer:", consumer
    message = consumer.consume_data() # type is generator
    print '-------------------------------->consumer topic is', KAFAKA_TOPIC
    cnt = 1
    for msg in message:
        #    67345
        print '********************', cnt, '********************'
        #    honeycomb-gw-shebao
        print 'msg.topic---------------->', msg.topic
        #    2
        print 'msg.partition---------------->', msg.partition
        #    127036
        print 'msg.offset---------------->', msg.offset
        #    -1
        print 'msg.timestamp---------------->', msg.timestamp
        #    0
        print 'msg.timestamp_type---------------->', msg.timestamp_type
        #    None
        print 'msg.key---------------->', msg.key
        
        #    {
        #       "request":"gbG9aPbUvNAXEDj8OxCP7HusUSquwY49AY70aaOOzkHFFseRZibvD8uhP1+HwegHOwQsFjO3bSJufgZgelM/Ks96xrg1QwsNVcIKexIQ2+KeEcrg36u1iPEj33DdE/E2l4niZZODULwvbMiXw6uQzaHSfJWto8a9qTZQyt9QWKeY/un/8ydhzvOzUNlg5Pkcx6hKGXqbqSyTKhjcAXFuSgsSlclEqX/8ftLW0Kvedb6Y+yn48mwGJiZtvt+N7t5opIk8oMKe1RrjAFbzLGHvJo4+gQpzLz5vWqZ9jlaUqjK9oz/BbQNV8WsCkBEon1ztYSjmDw1wB0rd538lqQG+cg==",
        #       "clientId":"CE-OL-TDC",
        #       "service":"shebao",
        #       "response":"{"errors":"成功","status":"1"}",
        #       "id":"2c4bb61918e346a6857631e4870fb6ca",
        #       "routingUrl":"http://10.130.85.117:8080/insure/api",
        #       "timestamp":"1516247687925"
        #    }        
        #   print 'msg.value---------------->', msg.value
        msg_value_jsons = json.loads(msg.value)
        msg_value_request = msg_value_jsons['request']
        msg_value_clientId = msg_value_jsons['clientId']
        msg_value_service = msg_value_jsons['service']
        msg_value_response = msg_value_jsons['response']
        msg_value_id = msg_value_jsons['id']
        msg_value_routingUrl = msg_value_jsons['routingUrl']
        msg_value_timestamp = msg_value_jsons['timestamp']
        
        print 'msg.value.request---------------->', msg_value_request
        print 'msg.value.clientId---------------->', msg_value_clientId
        print 'msg.value.service---------------->', msg_value_service
        print 'msg.value.response---------------->', msg_value_response
        print 'msg.value.id---------------->', msg_value_id
        print 'msg.value.routingUrl---------------->', msg_value_routingUrl
        print 'msg.value.timestamp---------------->', msg_value_timestamp
        
        #    -213619761
        print 'msg.checksum---------------->', msg.checksum
        #    -1
        print 'msg.serialized_key_size---------------->', msg.serialized_key_size
        #    574
        print 'msg.serialized_value_size---------------->', msg.serialized_value_size
        cnt = cnt + 1
        
        if (msg_value_service == 'taobao' or msg_value_service == 'taobaoCore') and 'alipayRecords' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ taobao/taobaoCore result data ~~~~~~~~~~~~~~~~~~~~'
        if msg_value_service == 'gjj' and 'brief' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ gjj result data ~~~~~~~~~~~~~~~~~~~~'
        if (msg_value_service == 'phone' or msg_value_service == 'phoneCore') and 'bills' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ phone/phoneCore result data ~~~~~~~~~~~~~~~~~~~~'
        if msg_value_service == 'pbccrc' and 'creditReportInfo' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ pbccrc result data ~~~~~~~~~~~~~~~~~~~~'
        if msg_value_service == 'mfa' and 'tranDetails' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ mfa result data ~~~~~~~~~~~~~~~~~~~~'
        if msg_value_service == 'cob' and 'creditDatas' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ cob result data ~~~~~~~~~~~~~~~~~~~~'
        if msg_value_service == 'baodan' and 'userinfo' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ baodan result data ~~~~~~~~~~~~~~~~~~~~'
        if msg_value_service == 'shebao' and 'userInfo' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ shebao result data ~~~~~~~~~~~~~~~~~~~~'
        if msg_value_service == 'dob' and 'debitDatas' in msg_value_response:
            print '~~~~~~~~~~~~~~~~~~~~ dob result data ~~~~~~~~~~~~~~~~~~~~'

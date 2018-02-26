# -*- coding:utf-8 -*-
from kafka import KafkaConsumer

if __name__ == '__main__':
    consumer = KafkaConsumer('honeycomb-gw-shebao',
                             group_id = 'xxo-test',
                             bootstrap_servers = '10.141.5.22,10.141.5.25,10.141.5.26:9092',
                             max_poll_records=50,
                             enable_auto_commit=False
                             )


    while True:
        print '====================================='
        raw_messages = consumer.poll(timeout_ms=1000, max_records=50)
        for topic_partition, messages in raw_messages.items():

            for msg in messages:
                print msg
                print type(msg.value)
                print msg.value





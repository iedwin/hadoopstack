#!/usr/bin/env bash


from pyhive import hive

cursor = hive.connect(host='myhadoop', auth='NOSASL', database='default').cursor()
cursor.execute('select * from users limit 10')
print(cursor.fetchone())
print(cursor.fetchall())
for i in range(7, 10):
    sql = "insert into users VALUEs ({},'username{}')".format(i, str(i))
    cursor.execute(sql)
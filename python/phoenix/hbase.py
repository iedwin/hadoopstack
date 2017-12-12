import jpype
import jaydebeapi
import os
phoenix_client_jar="phoenix-1.2.0-client.jar"
args= '-Djava.class.path={}'.format(phoenix_client_jar)
jvm_path=jpype.getDefaultJVMPath()
jpype.startJVM(jvm_path,args)
conn=jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver','jdbc:phoenix:fetch-slave1:2181',['',''],phoenix_client_jar)
curs=conn.cursor()
sql="select * from \"HServiceTest\""
count=curs.execute(sql)
results=curs.fetchall()
for r in results:
    print r
import jpype
import jaydebeapi
import os
phoenix_client_jar="mysql-connector-java-5.1.38-bin.jar"
args= '-Djava.class.path={}'.format(phoenix_client_jar)
jvm_path=jpype.getDefaultJVMPath()
jpype.startJVM(jvm_path,args)
conn=jaydebeapi.connect('com.mysql.jdbc.Driver','jdbc:mysql://fetch-master:3306/hive',['hive','hive'],phoenix_client_jar)
curs=conn.cursor()
# sql="select * from \"HServiceTest\""
sql="select * from VERSION"
count=curs.execute(sql)
results=curs.fetchall()
for r in results:
    print r
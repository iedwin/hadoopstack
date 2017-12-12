package com.xiaoxiaomo.cassandra;

import org.apache.thrift.transport.TTransport;
import org.junit.Test;

/**
 * Created by TangXD on 2017/11/14.
 */
public class TestCassandraThriftClient {


    //测试线程局部变量
    @Test
    public void testThreadLocal() throws Exception{
        TTransport t1 = CassandraThriftClient.openConnection();
        System.out.println(t1);
        TTransport t2 = CassandraThriftClient.openConnection();
        System.out.println(t2);

        new Thread(){
            public void run(){
                TTransport t3 = CassandraThriftClient.openConnection();
                System.out.println(t3);
                CassandraThriftClient.closeConnection();
            }
        }.start();

        Thread.sleep(100);
        CassandraThriftClient.closeConnection();
        System.out.println(t1.isOpen());
    }


    @Test
    public void testInsertSuperColumn() throws Exception{

        CassandraThriftClient.insertColumn("test1","2","name","xiaoxiaomo");
    }


}

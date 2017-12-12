package com.xiaoxiaomo.cassandra;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

public class CassandraThriftClient {
    /*
     * 对cassandra数据库的基础操作
     * */

    private static String host = "10.141.5.27";
    private static int port = 9160;
    private static String keyspace = "testspace";
    //暴露client供外界批量插入数据
    public static Cassandra.Client client = null;
    private static ThreadLocal<TTransport> ttrans = new ThreadLocal<TTransport>();

    //打开数据库连接
    public static TTransport openConnection(){
        TTransport tTransport = ttrans.get();
        if( tTransport == null ){
            tTransport = new TFramedTransport(new TSocket(host, port));
            TProtocol tProtocol = new TBinaryProtocol(tTransport);
            client = new Cassandra.Client(tProtocol);
            try {
                tTransport.open();
                client.set_keyspace(keyspace);
                ttrans.set(tTransport);
                System.out.println(tTransport);
            } catch (TTransportException e) {
                e.printStackTrace();
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
        return tTransport;
    }

    //关闭数据库连接
    public static void closeConnection(){
        TTransport tTransport = ttrans.get();
        ttrans.set(null);
        if( tTransport != null && tTransport.isOpen() )
            tTransport.close();
    }




    /*插入一个supercolumn(for super column family)
     * @param columns column的map集合
     * */
    public static void insertSuperColumn(String superColumnFamily, String key, String superName, Map<String, String> columns)
            throws UnsupportedEncodingException, InvalidRequestException,
            UnavailableException, TimedOutException, TException{
        openConnection();
        Map<ByteBuffer, Map<String, List<Mutation>>> map;
        map = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        List<Mutation> list = new ArrayList<Mutation>();
        SuperColumn superColumn = new SuperColumn();
        superColumn.setName(CassandraThriftClient.toByteBuffer(superName));
        Set<String> columnNames = columns.keySet();
        for(String columnName: columnNames) {
            Column c = new Column();
            c.setName(CassandraThriftClient.toByteBuffer(columnName));
            c.setValue(CassandraThriftClient.toByteBuffer(columns.get(columnName)));
            c.setTimestamp(System.currentTimeMillis());
            superColumn.addToColumns(c);
        }
        ColumnOrSuperColumn cos = new ColumnOrSuperColumn();
        cos.super_column = superColumn;
        Mutation mutation = new Mutation();
        mutation.column_or_supercolumn = cos;
        list.add(mutation);
        Map<String,List<Mutation>> supers = new HashMap<String, List<Mutation>>();
        supers.put(superColumnFamily, list);
        map.put(toByteBuffer(key), supers);
        client.batch_mutate(map, ConsistencyLevel.ONE);
        closeConnection();
    }

    //插入一个column(for standard column family)
    public static void insertColumn(String columnFamily, String key, String columnName, String columnValue) throws UnsupportedEncodingException{
        openConnection();
        ColumnParent parent = new ColumnParent(columnFamily);
        if( client != null ) {
            Column column = new Column( toByteBuffer(columnName) );
            column.setValue(toByteBuffer(columnValue));
            long timestamp = System.currentTimeMillis();
            column.setTimestamp(timestamp);
            try {
                client.insert(toByteBuffer(key), parent, column, ConsistencyLevel.ONE);
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            } catch (UnavailableException e) {
                e.printStackTrace();
            } catch (TimedOutException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
            closeConnection();
        }
    }

    /*获取key对应的column集合(for standard column family)
     * @return column的map集合
     * */
    public static HashMap<String, String> getColumns(String columnFamily, String key) throws InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException, TException{
        openConnection();
        ColumnParent parent = new ColumnParent(columnFamily);
        if( client != null ) {
            SlicePredicate predicate = new SlicePredicate();
            //定义查询的columnName范围(begin~end)，正反方向，数目(columnName在数据库中是排好序的，所以有正方向查询，反方向查询)
            SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false,100);
            predicate.setSlice_range(sliceRange);
            List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(key), parent, predicate, ConsistencyLevel.ONE);
            if( results == null )
                return null;
            HashMap<String, String> map = new HashMap<String, String>();
            for (ColumnOrSuperColumn result : results)
            {
                Column column = result.column;
                map.put(byteBufferToString(column.name), byteBufferToString(column.value));
                //System.out.println(byteBufferToString(column.name) + " : " + byteBufferToString(column.value) );
            }
            closeConnection();
            return map;
        }
        return null;
    }

    /*获取key对应的superColumn集合(for super column family)
     * @return superColumn的map集合
     * */
    public HashMap<String, Map<String, String>> getSuperColumns(String superColumnFamily, String key) throws InvalidRequestException, UnavailableException, TimedOutException, UnsupportedEncodingException, TException{
        openConnection();
        ColumnParent parent = new ColumnParent(superColumnFamily);
        if( client != null ) {
            SlicePredicate predicate = new SlicePredicate();
            //定义查询的superColumn.key范围，正反方向，数目(superColumn.key在数据库中是排好序的，所以有正方向查询，反方向查询)
            SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 100);
            predicate.setSlice_range(sliceRange);
            List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(key), parent, predicate, ConsistencyLevel.ONE);
            if( results == null )
                return null;
            HashMap<String, Map<String, String>> supers = new HashMap<String, Map<String,String>>();
            for (ColumnOrSuperColumn result : results)
            {
                SuperColumn superColumn = result.super_column;
                Map<String, String> columns = new HashMap<String, String>();
                for( Column column : superColumn.columns){
                    columns.put(byteBufferToString(column.name), byteBufferToString(column.value));
                }
                supers.put(byteBufferToString(superColumn.name), columns);
                //System.out.println(byteBufferToString(column.name) + " : " + byteBufferToString(column.value) );
            }
            closeConnection();
            return supers;
        }
        return null;
    }

    /*获取key，columnName对应的columnValue(for standard column family)
     * @return String
     * */
    public String getColumnValue(String columnFamily, String key, String columnName){
        try {
            ColumnPath path = new ColumnPath(columnFamily);
            path.setColumn(toByteBuffer(columnName));
            try {
                openConnection();
                String result =  new String( client.get(toByteBuffer(key), path, ConsistencyLevel.ONE).getColumn().getValue() );
                closeConnection();
                return result;
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            } catch (NotFoundException e) {
                e.printStackTrace();
            } catch (UnavailableException e) {
                e.printStackTrace();
            } catch (TimedOutException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    //String类型转化为ByteBuffer(UTF-8编码)
    public static ByteBuffer toByteBuffer(String value) throws UnsupportedEncodingException  {
        return ByteBuffer.wrap(value.getBytes("UTF-8"));
    }

    //ByteBuffer类型转化为String(UTF-8编码)
    public static String byteBufferToString(ByteBuffer byteBuffer) throws UnsupportedEncodingException{
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new String(bytes,"UTF-8");
    }
}
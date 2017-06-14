package com.xiaoxiaomo.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HBase 二级索引
 * Created by xiaoxiaomo on 2016/9/28.
 */
public class IndexUtils {
    public static final String IDX_TB_SUFFIX = "Idx" ;

    private static Connection connection;
    private static Map<String, HTable> hBaseMap = new HashMap<>();
    static {
        Configuration conf = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HTable getHTable(String tableName , boolean isIdxTable) throws IOException {
        //二级索引表名：原表名+Idx后缀
        if ( isIdxTable ) tableName += IndexUtils.IDX_TB_SUFFIX ;   //添加Idx后缀

        HTable hTable = hBaseMap.get(tableName);
        if (hTable == null) {
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            hBaseMap.put(tableName, hTable);
        }
        return hTable;
    }


    /**
     * 创建索引表
     * 索引表表名：基础表表名 + IDX_TB_SUFFIX
     * @param tableName 基础表表名
     * @param family 列族
     * @param isProduct 是否生产环境
     * @throws Exception
     */
    public static void createIdxTable(
            final String tableName , final String family,final boolean isProduct ) throws Exception{
        TableName indexTableName = TableName.valueOf(tableName+IDX_TB_SUFFIX) ;
        HTableDescriptor tableDesc = new HTableDescriptor(indexTableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes(family));
        if( isProduct )
            columnDesc.setCompressionType(Compression.Algorithm.SNAPPY) ;
        columnDesc.setMaxVersions(1);
        tableDesc.addFamily(columnDesc);
        createTable(indexTableName, tableDesc , getTablePartitions(tableName) );
    }

    public static void createTable(
            final TableName tableName ,final HTableDescriptor tableDesc, byte [][] splitKeys ) throws Exception{
        HBaseAdmin admin = new HBaseAdmin(connection);

        //为了动态建立分区,先删除表
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            admin.deleteTable(tableName);
        }
        admin.createTable(tableDesc,splitKeys);
        admin.close();
    }


    public static byte[][] getTablePartitions(final String tableName) throws IOException{
        HTable table = IndexUtils.getHTable(tableName , false) ;
        List<HRegionLocation> list = table.getAllRegionLocations();
        byte[][] splitKeys = new byte[list.size() - 1][];
        for (int i = 1; i <list.size(); i++) {
            splitKeys[i-1] = list.get(i).getRegionInfo().getStartKey() ;
        }
        return splitKeys;
    }

}

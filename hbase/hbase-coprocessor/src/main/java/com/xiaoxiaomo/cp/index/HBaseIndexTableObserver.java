package com.xiaoxiaomo.cp.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * HBase二级索引
 * 注意：协处理器目前存在一个bug，无法动态加载
 * 解决：（1）滚动重启regionserver，避免停掉所有的节点
 *      （2）改变协处理器的jar的类名字或者hdfs加载路径，以方便有新的ClassLoad去加载它
 *       官方：https://hbase.apache.org/book.html#cp
 * Created by xiaoxiaomo on 2016/9/22.
 */
public class HBaseIndexTableObserver extends BaseRegionObserver {

    public static final String IDX_TB_SUFFIX = "Idx" ;
    private static Connection connection;

    private Map<String, HTable> hBaseMap = new HashMap<>();

    static {
        Configuration configuration = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HTable getHTable(ObserverContext<RegionCoprocessorEnvironment> e  , boolean isIdxTable) throws IOException {

        /** 二级索引表名：原表名+Idx后缀 **/

        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        /** 添加Idx后缀 **/
        if ( isIdxTable ) tableName += IDX_TB_SUFFIX ;
        HTable hTable = hBaseMap.get(tableName);
        if (hTable != null) {
            return hTable;
        } else {
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
            hBaseMap.put(tableName, hTable);
            return hTable;
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {

        super.postPut(e, put, edit, durability);

         //HBase,put数据后，建立二级索引
        HTable idxTable = getHTable(e , true);
        Put idxPut = new Put(put.getRow());
        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
        for (byte[] bytes : familyCellMap.keySet()) {
            idxPut.addColumn(bytes , "".getBytes(),"".getBytes()) ;
        }

        idxTable.put( idxPut );
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability)
            throws IOException {

        byte[] rowKey = delete.getRow();
        super.postDelete(e, delete, edit, durability);

        HTable table = getHTable(e , false);
        Get get = new Get(rowKey);
        Result result = table.get(get);

        /** 当该列数据被完全删除时，则删除索引的rowKey **/
        if (result == null || result.isEmpty() ) {
            HTable idxTable = getHTable(e, true);
            Delete idxDelete = new Delete(rowKey);
            idxTable.delete(idxDelete);
        }
    }
}

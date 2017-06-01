package com.xiaoxiaomo.cp.count;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 协处理器 - 行统计
 * Created by xiaoxiaomo on 2017/5/26.
 */
public class HBaseRowCount {

    private static Logger logger = LoggerFactory.getLogger(HBaseRowCount.class) ;
    public static void main(String[] args) {

        //1. 参数，需要传入表名,列族
        if (args.length < 2) {
            System.err.println("ERROR: Wrong number of parameters: " + args.length );
            System.err.println("Usage: VideoIndexer <tableName> <family>");
            System.exit(-1);
        }

        String tableName = args[0];
        String columnFamily = args[1];

        //2. 实例化一个聚合客户端
        AggregationClient client = new AggregationClient(HBaseConfiguration.create());

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes( columnFamily )) ;

        long start= System.currentTimeMillis() ;
        long count = 0 ;
        //3. 统计
        try {
            count = client.rowCount( TableName.valueOf(tableName) , new LongColumnInterpreter(), scan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            logger.error( "异常" , throwable );
        }

        logger.info( count+ "，条数据耗时："+(System.currentTimeMillis() - start) );
    }
}

package com.xiaoxiaomo.hbase;

import com.xiaoxiaomo.hbase.utils.DateUtil;
import com.xiaoxiaomo.hbase.utils.IndexUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 建立HBase二级索引
 * 成功过20亿左右的数据
 * Created by xiaoxiaomo on 2016/9/23.
 */
public class HBaseSecondaryIndexJob extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSecondaryIndexJob.class);

    @Override
    public int run(String[] args) throws Exception {
        //配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);//推测执行
        conf.setInt("hbase.client.scanner.caching", 5000);//一次next()从服务器获取数据条数

        //参数，需要传入表名
        if (args.length < 3) {
            LOG.error("ERROR: Wrong number of parameters: " + args.length );
            LOG.error("Usage: VideoIndexer <tableName> <family> <qualifier>");
            System.exit(-1);
        }

        String tableName = args[0];
        String family = args[1];
        String qualifier = args[2];
        String isCreateIdxTB = args[3];

        conf.set("tableName", tableName );
        conf.set("family", family );

        Job job = Job.getInstance(
                conf, "HBaseSecondaryIndex_" + tableName  +"_"+family + " Job @ " + DateUtil.getNowTime());
        job.setJarByClass(HBaseSecondaryIndexJob.class);

        //通过主表创建索引表并分区
        if( isCreateIdxTB != null && "true".equals(isCreateIdxTB) )
            IndexUtils.createIdxTable( tableName ,family ,true );

        Scan scan = new Scan();
        scan.setCacheBlocks(false); // scan的数据不放在缓存中，一次性的
        scan.addColumn(family.getBytes(),qualifier.getBytes()) ;

        job.setOutputFormatClass(MultiTableOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(
                tableName, scan, Map.class, NullWritable.class,NullWritable.class, job);
        job.setNumReduceTasks(0); // 无reduce任务

        if ( job == null ) {
            System.exit(-1);
        }

        //提交
        return  job.waitForCompletion(true) ? 0 : 1;
    }


    public static class Map extends TableMapper<NullWritable,NullWritable> {
        Configuration conf = null;
        HTable idxTab = null;
        private String family;
        private List<Put> puts = new ArrayList<>();
        private Put put = null;
        static long count = 0;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            conf = context.getConfiguration();
            idxTab = new HTable(conf, conf.get("tableName") + IndexUtils.IDX_TB_SUFFIX);
            idxTab.setAutoFlushTo(false);
            idxTab.setWriteBufferSize(12 * 1024 * 1024);
            family = conf.get("family");
        }

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context)
                throws IOException, InterruptedException {
            put = new Put(values.getRow()); //原始表的rowKey为索引表的rowKey
            put.addColumn(Bytes.toBytes(family), "".getBytes(), "".getBytes());
            put.setWriteToWAL(false);
            puts.add(put) ;
            if ((++count % 20000) == 0) {
                idxTab.put(puts) ;
                puts.clear();
                context.setStatus( "已经插入数据：" + count);
                context.progress();
                LOG.info("已经插入数据：" + count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            if( puts != null && puts.size() > 0 ){
                idxTab.put(puts) ;
            }
            super.cleanup(context);
            idxTab.flushCommits();
            idxTab.close();
        }
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseSecondaryIndexJob(), args);
        System.exit(exitCode);
    }
}

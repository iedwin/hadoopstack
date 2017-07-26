package com.xiaoxiaomo.hbase;

import com.xiaoxiaomo.utils.DateUtil;
import com.xiaoxiaomo.hbase.utils.HFileLoader;
import com.xiaoxiaomo.hbase.utils.IndexUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 通过BulkLoad 从原表中导入 索引
 * 适合初次导入数据
 * Created by xiaoxiaomo on 2016/9/28.
 */
public class BulkLoadHBase2HBaseSecIndexJob extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadHBase2HBaseSecIndexJob.class);

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new BulkLoadHBase2HBaseSecIndexJob(), args);
            if(exitCode == 0) {
                LOG.info("Job is successfully completed...");
            } else {
                LOG.info("Job failed...");
            }
            System.exit(exitCode);
        } catch(Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage() , e );
            System.exit(-1);
        }
    }


    public int run(String[] args) throws Exception {

        //配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);//推测执行
        conf.setInt("hbase.client.scanner.caching", 5000);//一次next()从服务器获取数据条数

        //参数，需要传入表名
        if (args.length < 3) {
            LOG.error("ERROR: Wrong number of parameters: " + args.length );
            LOG.error("Usage: VideoIndexer <tableName> <family> <outputPath>");
            System.exit(-1);
        }

        String tableName = args[0];
        String family = args[1];
        String outputPath = args[2];

        conf.set("tableName", tableName);
        conf.set("family", family);

        Job job = Job.getInstance(conf, "HBaseBulkLoad2SecIndex_" + tableName  +"_"+family + " Job @ " + DateUtil.getNowTime());
        job.setJarByClass(BulkLoadHBase2HBaseSecIndexJob.class);

        //通过主表创建索引表并分区
        IndexUtils.createIdxTable( tableName ,family ,true);

        //检索数据
        Scan scan = new Scan();
        scan.setCacheBlocks(false); // scan的数据不放在缓存中，一次性的
        scan.addFamily(family.getBytes());

        job.setOutputFormatClass(HFileOutputFormat2.class);
        TableMapReduceUtil.initTableMapperJob(
                tableName, scan, BulkLoadMapper.class, ImmutableBytesWritable.class,Put.class, job);

        FileSystem fs = FileSystem.get(conf);
        Path output = new Path(outputPath);
        if (fs.exists(output)) {
            fs.delete(output, true);//如果输出路径存在，就将其删除
        }

        FileOutputFormat.setOutputPath(job, output);//输出路径

        //一定记得建 HBase 数据表时做 Region 的预切分，
        // HFileOutputFormat.configureIncrementalLoad 方法会根据 Region 的数量来决定 Reduce 的数量
        // 每个 Reduce 覆盖的 RowKey 范围，否则单个 Reduce 过大，容易造成任务处理不均衡
        HFileOutputFormat2.configureIncrementalLoad(job, IndexUtils.getHTable(tableName,true),IndexUtils.getHTable(tableName, true));
        job.waitForCompletion(true);
        if (job.isSuccessful()){
            //加载HFile文件到目标索引表
            HFileLoader.doBulkLoad(outputPath, tableName+IndexUtils.IDX_TB_SUFFIX , family);//导入数据
            return 0;
        } else {
            return 1;
        }
    }


    public static class BulkLoadMapper extends TableMapper<ImmutableBytesWritable,Put> {
        private ImmutableBytesWritable rowKey ;
        private String family;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            family = conf.get("family");
        }

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context)
                throws IOException, InterruptedException {
            rowKey = new ImmutableBytesWritable(values.getRow()) ;
            Put put = new Put(values.getRow()) ; //原始表的rowKey为索引表的rowKey
            put.addColumn(Bytes.toBytes(family), "".getBytes(), "".getBytes());
            context.write(rowKey, put);
        }

    }

}

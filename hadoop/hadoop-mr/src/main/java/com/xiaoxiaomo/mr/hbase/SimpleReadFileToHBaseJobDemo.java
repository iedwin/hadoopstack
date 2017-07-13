package com.xiaoxiaomo.mr.hbase;

import com.xiaoxiaomo.mr.constants.ConstantsTableInfo;
import com.xiaoxiaomo.utils.DateUtil;
import com.xiaoxiaomo.utils.MD5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 读取文件内容信息，插入到HBase中
 * <p>
 * Created by xiaoxiaomo on 2017/6/1.
 */
public class SimpleReadFileToHBaseJobDemo extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleReadFileToHBaseJobDemo.class);
    private static List<String> eventColumn;

    @Override
    public int run(String[] args) throws Exception {

        /** 1. 加载配置 **/
        Configuration config = HBaseConfiguration.create();

        config.setBoolean("mapred.map.tasks.speculative.execution", false);
        config.setDouble("mapreduce.reduce.shuffle.input.buffer.percent", 0.5);

        String inputPath = args[0];
        String outputTable = args[1];

        Job job = Job.getInstance(config, "SimpleReadFileToHBase Job @ " + DateUtil.getNowTime());
        job.setJarByClass(SimpleReadFileToHBaseJobDemo.class);

        /** 2. 文件输入 **/
        FileInputFormat.setInputPaths(job, new Path(inputPath));

        job.setMapperClass(putHBaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /** 3. 通过TableMapReduceUtil 插入到HBase **/
        TableMapReduceUtil.initTableReducerJob(outputTable, putHBaseReducer.class, job);
        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class putHBaseMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            /** 这里通过文件名去匹配找到HBase中的字段 **/
            InputSplit inputSplit = context.getInputSplit();
            String file = ((FileSplit) inputSplit).getPath().toString();
            LOG.info("当前map处理的文件为" + file);

            /** 找到HBase中的字段 **/
            if (file.contains(ConstantsTableInfo.TABLE_NAME)) {
                eventColumn = ConstantsTableInfo.TABLE_COLUMN;
            }
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (eventColumn == null || value == null) {
                return;
            }

            String[] splits = value.toString().split(ConstantsTableInfo.LINE_CHAR);

            if (splits.length != eventColumn.size()) {
                LOG.error("模板长度和数据长度不同,无法处理");
                return;
            }

            context.write(new Text(splits[0]), value); //splits[0] = uuid
        }
    }

    static class putHBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        @SuppressWarnings("rawtypes")
        @Override
        protected void reduce(
                Text key,
                Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {

            String uid = key.toString();

            for (Text value : values) {

                /** 获取value , 并作一些判断 **/
                String[] split = value.toString().split(ConstantsTableInfo.LINE_CHAR);

                /** 获取数据 **/
                Map<String, String> data = new HashMap<>();

                for (int i = 0, j = eventColumn.size(); i < j; i++) {
                    if (!"-".equals(split[i]) && !"NULL".equals(split[i])
                            && split[i] != null
                            && split[i].length() != 0) {
                        data.put(eventColumn.get(i), split[i]);
                    }
                }

                /** TODO: 插入数据，单条插入**/
                byte[] rowKey = MD5.encryption(uid).getBytes();

                Put put = new Put(rowKey);
                for (Map.Entry entry : data.entrySet()) {
                    put.addColumn(
                            ConstantsTableInfo.COLUMN_FAMILY.getBytes(),
                            entry.getKey().toString().getBytes(),
                            entry.getValue().toString().getBytes());
                }

                context.write(null, put);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SimpleReadFileToHBaseJobDemo(), args);
        System.exit(exitCode);

    }

}

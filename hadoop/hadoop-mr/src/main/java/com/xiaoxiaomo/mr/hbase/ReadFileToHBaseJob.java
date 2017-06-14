package com.xiaoxiaomo.mr.hbase;

import com.xiaoxiaomo.utils.DateUtil;
import com.xiaoxiaomo.utils.MD5;
import com.xiaoxiaomo.mr.constants.ConstantsTableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 *
 * 读取文件内容信息，插入到HBase中
 *
 * Created by xiaoxiaomo on 2017/6/1.
 */
public class ReadFileToHBaseJob extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(ReadFileToHBaseJob.class);
    private static List<String> eventColumn;
    @Override
    public int run(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        config.setBoolean("mapred.map.tasks.speculative.execution", false);
        config.setDouble("mapreduce.reduce.shuffle.input.buffer.percent", 0.5);

        String inputPath = args[0];
        String outputTable = args[1];

        Job job = Job.getInstance(config, "ReadFileToHBase Job @ "+ DateUtil.getNowTime());
        job.setJarByClass(ReadFileToHBaseJob.class);

        //文件输入
        FileInputFormat.setInputPaths(job, new Path(inputPath));

        job.setMapperClass(putHBaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //通过TableMapReduceUtil 插入到HBase
        TableMapReduceUtil.initTableReducerJob(outputTable,putHBaseReducer.class, job);
        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class putHBaseMapper extends Mapper<Object, Text, Text, Text> {


        private Text textKey = new Text();

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String file = ((FileSplit) inputSplit).getPath().toString();
            logger.info("当前map处理的文件为" + file);

            //可以在这里做一些判断
            if (file.contains(ConstantsTableInfo.TABLE_NAME)) {
                eventColumn = ConstantsTableInfo.TABLE_COLUMN;
            }
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            if (eventColumn == null) {
                return;
            }
            String line = value.toString();

            if (line.endsWith(ConstantsTableInfo.LINE_CHAR)
                    && line.split(ConstantsTableInfo.LINE_CHAR).length < eventColumn
                    .size()) {
                line = line + "-";
            }
            List<String> words = new ArrayList<String>();
            for (String val : line.split(ConstantsTableInfo.LINE_CHAR)) {
                words.add(val);
            }
            if (words.size() != eventColumn.size()) {
                logger.error("模板长度和数据长度不同,无法处理");
                return;
            }

            Map<String, String> data = new HashMap<String, String>();
            for (int i = 0, j = eventColumn.size(); i < j; i++) {
                if (!"-".equals(words.get(i))) {
                    data.put(eventColumn.get(i), words.get(i));
                }
            }
            textKey.set(data.get("uid"));
            context.write(textKey, value);
        }
    }

    static class putHBaseReducer extends
            TableReducer<Text, Text, ImmutableBytesWritable> {

        @SuppressWarnings("rawtypes")
        @Override
        protected void reduce(
                Text key,
                Iterable<Text> values,
                Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
                throws IOException, InterruptedException {

            String uid = key.toString();

            String event = key.toString().split(ConstantsTableInfo.LINE_CHAR)[3];


            for (Text value : values) {

                //获取value , 并作一些判断
                String line = value.toString();

                if (line.endsWith(ConstantsTableInfo.LINE_CHAR)
                        && line.split(ConstantsTableInfo.LINE_CHAR).length < eventColumn
                        .size()) {
                    line = line + "-";
                }
                List<String> words = new ArrayList<>();
                for (String val : line.split(ConstantsTableInfo.LINE_CHAR)) {
                    words.add(val);
                }
                if (words.size() != eventColumn.size()) {
                    logger.error("数据格式有误:" + line);
                    return;
                }

                /** 获取数据 **/
                Map<String, String> data = new HashMap<>();

                for (int i = 1, j = eventColumn.size(); i < j; i++) {/**mysql-中id不存HBase**/
                    if (!"-".equals(words.get(i))
                            && !"NULL".equals(words.get(i))
                            && words.get(i) != null
                            && words.get(i).length() != 0) {

                        data.put(eventColumn.get(i), words.get(i));
                    }
                }

                /**插入数据，下面为单条插入，可优化**/
                byte[] rowKey = MD5.encryption(uid).getBytes();

                Put put = new Put(rowKey);
                put.addColumn(ConstantsTableInfo.COLUMN_FAMILY.getBytes(),
                        "event".getBytes(), event.getBytes());

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
        int exitCode = ToolRunner.run(new ReadFileToHBaseJob(), args);
        System.exit(exitCode);

    }

}

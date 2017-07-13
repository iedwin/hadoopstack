package com.xiaoxiaomo.mr.hive;

import com.alibaba.fastjson.JSONObject;
import com.xiaoxiaomo.utils.MD5;
import com.xiaoxiaomo.mr.constants.ConstantsTableInfo;
import com.xiaoxiaomo.mr.utils.kafka.CheckpointManager;
import com.xiaoxiaomo.mr.utils.kafka.io.KafkaInputFormat;
import com.xiaoxiaomo.mr.utils.kafka.io.MsgMetadataWritable;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 *
 * 同 Kafka2HiveJobDemo
 *
 *
 * Created by xiaoxiaomo on 2017/6/1.
 */
public class Kafka2HiveParquetDemo extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(Kafka2HiveParquetDemo.class);
    private static String topic;

    @Override
    public int run(String[] args) throws Exception {
        //conf优化设置
        Configuration conf = getConf();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);


        //获取可配置参数
        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        CommandLine cmd = parser.parse(options, args);


        //必须参数检验,其中日期一般来自于oozie传递,当进行补数操作时,需手动传入
        if (cmd.hasOption("h") || !cmd.hasOption("d") || !cmd.hasOption("p") || !cmd.hasOption("t")) {
            printHelpAndExit(options);
        }

        //加载公共配置
        Properties props = new Properties();
        props.load(Kafka2HiveParquetDemo.class.getResourceAsStream("/kafka.properties"));
        CheckpointManager.configureUseZooKeeper(conf, props.getProperty("consumerGroupId"));
        KafkaInputFormat.configureZkConnection(conf, props.getProperty("zk.connect"));

        //topic设置
        topic = cmd.getOptionValue("topic");
        KafkaInputFormat.configureKafkaTopics(conf, topic);
        if (cmd.hasOption("o")) {
            KafkaInputFormat.configureAutoOffsetReset(conf, cmd.getOptionValue("offset"));
        }

        //日期设置
        String etlDate = cmd.getOptionValue("date");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            simpleDateFormat.parse(etlDate);
            conf.set(KafkaInputFormat.CONFIG_ETL_DATE, etlDate);
        } catch (Exception ex) {
            logger.error("Invalid date:{},must be:yyyy-MM-dd", etlDate);
            printHelpAndExit(options);
        }

        //输入路径设置
        String output = cmd.getOptionValue("outPath");
        String hdfsPath = output + "/" + etlDate;

        JobConf jobConf = new JobConf(conf);
        jobConf.setJarByClass(getClass());
        Job job = Job.getInstance(jobConf, "TableNameETL");

        job.setInputFormatClass(KafkaInputFormat.class);

        job.setOutputFormatClass(ParquetOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(KafkaETLMapper.class);
        job.setReducerClass(KafkaETLReducer.class);
        job.setNumReduceTasks(5);

        Path outputPath = new Path(hdfsPath);
        final FileSystem fileSystem = outputPath.getFileSystem(conf);
        fileSystem.delete(outputPath, true);
        ParquetOutputFormat.setOutputPath(job, outputPath);
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
        GroupWriteSupport.setSchema(
                MessageTypeParser.parseMessageType(ConstantsTableInfo.SCHEMA.get(topic)),
                job.getConfiguration());

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class KafkaETLMapper extends Mapper<MsgMetadataWritable, BytesWritable, Text, Text> {
        @Override
        protected void map(MsgMetadataWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            String record = new String(value.getBytes(), 0, value.getLength());
            context.write(new Text(MD5.encryption(record)), new Text(record));
        }
    }

    public static class KafkaETLReducer extends Reducer<Text, Text, NullWritable, Group> {
        private SimpleGroupFactory factory;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> records = new HashSet<>();
            //去除队列中可能的重复
            for (Text value : values) {
                records.add(value.toString());
            }
            for (String record : records) {

                Group group = factory.newGroup();
                JSONObject recordMap = JSONObject.parseObject(record) ;
                for (Object k : recordMap.keySet()) {
                    group.append(k.toString(),recordMap.get(k).toString());
                }

                context.write(null, group);
            }
        }
    }


    private void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Kafka2HiveParquetDemo", options);
        System.exit(0);
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
        Options options = new Options();

        options.addOption(OptionBuilder
                .withLongOpt("topic")
                .hasArg()
                .withDescription("Required!Kafka etl topic name")
                .create("t"));

        //同Kafka2HiveJobDemo
        return options;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Kafka2HiveParquetDemo(), args);
        System.exit(exitCode);
    }
}
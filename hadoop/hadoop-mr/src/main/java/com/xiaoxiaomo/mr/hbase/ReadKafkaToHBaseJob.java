package com.xiaoxiaomo.mr.hbase;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonSyntaxException;
import com.xiaoxiaomo.utils.MD5;
import com.xiaoxiaomo.mr.constants.ConstantsTableInfo;
import com.xiaoxiaomo.mr.utils.kafka.CheckpointManager;
import com.xiaoxiaomo.mr.utils.kafka.io.KafkaInputFormat;
import com.xiaoxiaomo.mr.utils.kafka.io.MsgMetadataWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 读取kafka数据存入HBase中
 *
 * Created by xiaoxiaomo on 2017/6/1.
 */
public class ReadKafkaToHBaseJob extends Configured implements Tool {

	private static final Logger logger = LoggerFactory
			.getLogger(ReadKafkaToHBaseJob.class);
    
	 private static final String output = "/tmp/output_kafka";

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		String kafkaDate = args[0];
		String outputTable = args[1];
		String kafkaTopic = args[2];

		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

        /** 加载配置 **/
		Properties props = new Properties();
		props.load(ReadKafkaToHBaseJob.class.getResourceAsStream("/kafka.properties"));
		CheckpointManager.configureUseZooKeeper(conf,props.getProperty("consumerGroupId"));
		KafkaInputFormat.configureZkConnection(conf,props.getProperty("zk.connect"));
		KafkaInputFormat.configureKafkaTopics(conf, kafkaTopic);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        /** KafkaInputFormat 【重点】 **/
        try {
            simpleDateFormat.parse(kafkaDate);
            conf.set(KafkaInputFormat.CONFIG_ETL_DATE, kafkaDate);
        } catch (Exception ex) {
            logger.error("date:{},must be:yyyy-MM-dd", kafkaDate);
        }
        String temp_output = output + "/" + kafkaDate.toString();

		Job job = Job.getInstance(conf, "ReadKafkaToHBaseJob");

		job.setJarByClass(ReadKafkaToHBaseJob.class);
		job.setInputFormatClass(KafkaInputFormat.class);
        
		job.setMapperClass(KafkaToHBaseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

        /** TableMapReduceUtil **/
		TableMapReduceUtil.initTableReducerJob(outputTable,KafkaToHBaseReducer.class, job);

        
		Path tmp = new Path(temp_output);
		FileOutputFormat.setOutputPath(job, tmp);
		final FileSystem fileSystem = tmp.getFileSystem(conf);
		fileSystem.delete(tmp, true);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 *
	 */
	static class KafkaToHBaseMapper extends
			Mapper<MsgMetadataWritable, BytesWritable, Text, Text> {

		private List<String> eventColumn;
		private Text textKey = new Text();
		private Text textValue = new Text();

		@Override
		protected void map(
				MsgMetadataWritable key,
				BytesWritable value,
				Mapper<MsgMetadataWritable, BytesWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String record = new String(value.getBytes(), 0, value.getLength());
			try {
				JSONObject recordMap = JSONObject.parseObject(record);
				if (recordMap.containsKey("event")) {
					String event = (String) recordMap.get("event");
					//可以在这里做一些判断
					if (event.contains(ConstantsTableInfo.TABLE_NAME)) {
						eventColumn = ConstantsTableInfo.TABLE_COLUMN;
					}

					Map<String, String> data = new HashMap<String, String>();
					for (int i = 1, j = eventColumn.size(); i < j; i++) {
						if (recordMap.get(eventColumn.get(i)) != null
								&& !"-".equals(recordMap.get(eventColumn.get(i)).toString())) {
							data.put(eventColumn.get(i),
							recordMap.get(eventColumn.get(i)).toString());
						}
					}
					String customerID = data.get("uid");

					textKey.set(customerID.toString());
					textValue.set(record);

					context.write(textKey, textValue);
				}
			} catch (InterruptedException e) {
				logger.error("context write get a error:" + record);
			} catch (JsonSyntaxException e) {
				logger.error("record is not a json:" + record);
			}
		}
	}

	static class KafkaToHBaseReducer extends
			TableReducer<Text, Text, ImmutableBytesWritable> {

		private List<String> eventColumn;

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

                JSONObject recordMap = JSONObject.parseObject(value.toString());
                Map<String, String> data = new HashMap<>();

                for (int i = 1, j = eventColumn.size(); i < j; i++) {
                    if (!"-".equals(recordMap
                            .get(eventColumn.get(i)))
                            && !"NULL".equals(recordMap.get(eventColumn.get(i)))
                            && recordMap.get(eventColumn.get(i)) != null
                            && recordMap.get(eventColumn.get(i))
                            .toString().length() != 0) {

                        data.put(eventColumn.get(i),
                                recordMap.get(eventColumn.get(i).toString())
                                        .toString());
                    }
                }
                byte[] rowKey = MD5.encryption(uid).getBytes();
                Put put = new Put(rowKey);
                put.addColumn(ConstantsTableInfo.COLUMN_FAMILY.getBytes(),
                        "event".getBytes(), event.getBytes());
                for (Map.Entry entry : data.entrySet()) {
                    put.addColumn(ConstantsTableInfo.COLUMN_FAMILY.getBytes(),
                            entry.getKey().toString().getBytes(), entry
                                    .getValue().toString().getBytes());
                }

                context.write(null, put);
            }
        }

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ReadKafkaToHBaseJob(), args);
		System.exit(exitCode);
	}

}

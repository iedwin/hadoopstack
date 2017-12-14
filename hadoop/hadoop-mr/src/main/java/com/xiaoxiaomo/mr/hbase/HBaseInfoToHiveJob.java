package com.xiaoxiaomo.mr.hbase;

import com.xiaoxiaomo.common.date.DateUtil;
import com.xiaoxiaomo.mr.constants.ConstantsTableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 *
 * HBase数据按照dt分区(月)存入hive仓库
 *
 * Created by xiaoxiaomo on 2017/6/1.
 */
public class HBaseInfoToHiveJob extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(HBaseInfoToHiveJob.class);

	@Override
	public int run(String[] args) throws Exception {

		//1. 配置信息
		Configuration conf = HBaseConfiguration.create();
		conf.setInt("hbase.client.scanner.caching", 5000);// 一次next()从服务器获取数据条数
		conf.set("hBaseTable", args[1]);

		//2. job信息
		Job job = Job.getInstance(conf,args[1] + " HBaseInfoToHiveJob" + DateUtil.getNowTime());
		job.setJarByClass(HBaseInfoToHiveJob.class);

		//3. 组装scan
		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.addFamily(Bytes.toBytes(ConstantsTableInfo.COLUMN_FAMILY));

		//4. 通过TableMapReduceUtil 获取HBase数据
		TableMapReduceUtil.initTableMapperJob(
				args[1],
				scan,
				HBaseToHiveMapper.class,
				Text.class,
				Text.class,
				job,
				false);

		job.setReducerClass(HBaseToHiveReducer.class);

		//5. map & reduce
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		//6. 输出到HDFS路径中
		Path output = new Path(args[0]);
		final FileSystem fileSystem = output.getFileSystem(conf);
		fileSystem.delete(output, true);

		TextOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class HBaseToHiveMapper extends TableMapper<Text, Text> {
		private String hBaseTable;
		private List<String> eventColumn;

		@Override
		protected void setup(
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {

			hBaseTable = context.getConfiguration().get("hBaseTable")
					.toLowerCase();
			logger.info("表" + hBaseTable);

			if (hBaseTable.contains(ConstantsTableInfo.TABLE_NAME)) {
				eventColumn = ConstantsTableInfo.TABLE_COLUMN;
			}
			super.setup(context);
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result value,Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			Map<String, String> m = new HashMap<>();

			for (Entry<byte[], byte[]> entry :
					value.getFamilyMap(ConstantsTableInfo.COLUMN_FAMILY.getBytes()).entrySet()) {
				String str = new String(entry.getValue());
				m.put(new String(entry.getKey()), str);
			}

			String mapValue;
			for (String val : eventColumn) {
				mapValue = m.get(val);
				sb.append(mapValue != null ? mapValue : "-");
				sb.append(ConstantsTableInfo.LINE_CHAR);
			}

			String columnDate = "";
			if (eventColumn.contains("createTime")) {
				columnDate = m.get("createTime");
			} else if (eventColumn.contains("orderTime")) {
				columnDate = m.get("orderTime");
			} else if (eventColumn.contains("registerTime")) {
				columnDate = m.get("registerTime");
			} else if (eventColumn.contains("tradeDate")) {
				columnDate = m.get("tradeDate");
			}

			if (columnDate == null || columnDate.length() == 0) {
				columnDate = "2014-01-01";
			}else {
				columnDate = columnDate.replace("年", "-").replace("月", "-").replace("日", "-");
			}
			String dt = columnDate.split(" ")[0];
			try {
				dt = convertMonthByTime(columnDate);
			} catch (ParseException e) {
				logger.info("date format error !",e);
			}

			sb.deleteCharAt(sb.length() - 1);
			context.write(new Text(dt), new Text(new String(sb)));
		}

		/**
		 * 根据根据给出的时间字符串 得出 所在月 1日 yyyy-MM-dd
		 * 
		 * @param d
		 * @throws ParseException
		 * @return
		 */
		private String convertMonthByTime(String d) throws ParseException {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date time = sdf.parse(d);

			Calendar cal = Calendar.getInstance();
			cal.setTime(time);
			cal.set(Calendar.DAY_OF_MONTH, 1);

			return sdf.format(cal.getTime());
		}
	}

	public static class HBaseToHiveReducer extends Reducer<Text, Text, NullWritable, Text> {

		private MultipleOutputs<NullWritable, Text> out;

		@Override
		protected void setup(Context context) {
			out = new MultipleOutputs<>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String path = key.toString() + "/" + key.toString();
			for (Text val : values) {
				out.write(NullWritable.get(), new Text(val), path);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			out.close();
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HBaseInfoToHiveJob(), args);
		System.exit(exitCode);
	}

}

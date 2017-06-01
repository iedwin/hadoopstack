
package com.xiaoxiaomo.mr.utils.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * Des:消费Kafka至hdfs的Mapper,后续考虑直接输出Parquet文件
 */
public class KafkaMapper extends Mapper<MsgMetadataWritable, BytesWritable, MsgMetadataWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(KafkaMapper.class);

    private static final String CONFIG_TIMESTAMP_EXTRACTOR_CLASS = "mapper.timestamp.extractor.class";

    private TimestampExtractor extractor;

    public static void configureTimestampExtractor(Configuration conf, String className) {
        conf.set(CONFIG_TIMESTAMP_EXTRACTOR_CLASS, className);
    }

    public static boolean isTimestampExtractorConfigured(Configuration conf) {
        return !conf.get(CONFIG_TIMESTAMP_EXTRACTOR_CLASS, "").equals("");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        try {
            //可选的输出路径设置
            Class<?> extractorClass = conf.getClass(CONFIG_TIMESTAMP_EXTRACTOR_CLASS, null);
            if (extractorClass != null) {
                extractor = extractorClass.asSubclass(TimestampExtractor.class).newInstance();
                log.info("Using timestamp extractor " + extractor);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        super.setup(context);
    }

    @Override
    public void map(MsgMetadataWritable key, BytesWritable value, Context context) throws IOException {
        try {
            if (key != null) {
                MsgMetadataWritable outputKey = key;
                if (extractor != null) {
                    Long timestamp = extractor.extract(key, value);
                    outputKey = new MsgMetadataWritable(key, timestamp);
                }
                BytesWritable outputValue = value;
                context.write(outputKey, outputValue);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw e;
        }
    }

}
package com.xiaoxiaomo.mr.utils.kafka.io;

import com.xiaoxiaomo.mr.utils.kafka.CheckpointManager;
import com.xiaoxiaomo.mr.utils.kafka.KafkaZkUtils;
import com.xiaoxiaomo.utils.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 参考：https://github.com/amient/kafka-hadoop-loader
 *
 * 自定义KafkaInputFormat
 */
public class KafkaInputFormat extends InputFormat<MsgMetadataWritable, BytesWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class) ;
    private static final String CONFIG_ZK_CONNECT_TIMEOUT_MS = "kafka.zookeeper.connection.timeout.ms";
    private static final String CONFIG_KAFKA_TOPIC_LIST = "kafka.topics";

    //TODO maybe:`earliest`, `latest` ,`checkpoint`
    static final String CONFIG_KAFKA_AUTOOFFSET_RESET = "kafka.offset.reset";
    static final String CONFIG_KAFKA_MESSAGE_MAX_BYTES = "kafka.fetch.message.max.bytes";
    static final String CONFIG_KAFKA_SOCKET_TIMEOUT_MS = "kafka.socket.timeout.ms";
    static final String CONFIG_KAFKA_RECEIVE_BUFFER_BYTES = "kafka.socket.receive.buffer.bytes";

    static final String CONFIG_ZK_CONNECT = "kafka.zookeeper.connect";
    static final String CONFIG_ZK_SESSION_TIMEOUT_MS = "kafka.zookeeper.session.timeout.ms";

    public static final String CONFIG_ETL_DATE = "etl.date";

    public static void configureKafkaTopics(Configuration conf, String comaSeparatedTopicNames) {
        conf.set(KafkaInputFormat.CONFIG_KAFKA_TOPIC_LIST, comaSeparatedTopicNames);
    }

    public static void configureZkConnection(Configuration conf, String zkConnectString) {
        conf.set(CONFIG_ZK_CONNECT, zkConnectString);
    }

    public static void configureZkTimeouts(Configuration conf, int sessionTimeoutMs, int connectTimeoutMs) {
        conf.setInt(CONFIG_ZK_SESSION_TIMEOUT_MS, sessionTimeoutMs);
        conf.setInt(CONFIG_ZK_CONNECT_TIMEOUT_MS, connectTimeoutMs);
    }

    public static void configureAutoOffsetReset(Configuration conf, String optionValue) {
        conf.set(CONFIG_KAFKA_AUTOOFFSET_RESET, optionValue);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        LOG.info("Kafka InputFormat 获取Splits Start!");
        try (KafkaZkUtils zkUtils = new KafkaZkUtils(
                conf.get(KafkaInputFormat.CONFIG_ZK_CONNECT),
                conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000),
                conf.getInt(KafkaInputFormat.CONFIG_ZK_CONNECT_TIMEOUT_MS, 10000)
        )) {
            return createSplitsForPartitionLeader(conf, zkUtils);
        } catch (Exception e){
            e.printStackTrace();
            return new ArrayList<>() ;
        }
    }

    @Override
    public RecordReader<MsgMetadataWritable, BytesWritable> createRecordReader(
            InputSplit inputSplit,
            TaskAttemptContext context
    ) throws IOException, InterruptedException {
        return new KafkaInputRecordReader();
    }

    /**
     * 从kafka分区leader中获取Splits
     *
     * @param conf job conf
     * @param zk   zkUtils
     * @return List<InputSplit>
     * @throws IOException
     */
    private List<InputSplit> createSplitsForPartitionLeader(Configuration conf, KafkaZkUtils zk) throws IOException {
        String[] inputTopics = conf.get(CONFIG_KAFKA_TOPIC_LIST).split(",");
        LOG.info(" 从kafka分区leader中，获取Split List 信息 ");
        String etlDate = conf.get(CONFIG_ETL_DATE);
        String nextDate = DateUtil.getTomorrow(etlDate, "yyyy-MM-dd");

        CheckpointManager checkpoints = new CheckpointManager(conf, zk);

        List<InputSplit> splits = new ArrayList<>();
        for (String topic : inputTopics) {
            Map<Integer, Integer> partitionLeaders = zk.getPartitionLeaders(topic);
            LOG.info(" PartitionLeaders Size:"+partitionLeaders.size());
            for (int partition : partitionLeaders.keySet()) {

                int brokerId = partitionLeaders.get(partition);

                long startOffset = checkpoints.getOffsetToConsume(topic, partition, etlDate);
                long endOffset = checkpoints.getOffsetToConsume(topic, partition, nextDate);
                if (startOffset < 0) {
                    LOG.debug("通过etlDate({})获取到startOffset={} < 0,然后不传日期重新获取startOffset ",etlDate,startOffset);
                    startOffset = checkpoints.getOffsetToConsume(topic, partition, "") + 1;
                }

                KafkaInputSplit split = new KafkaInputSplit(
                        brokerId,
                        zk.getBrokerName(brokerId),
                        topic,
                        partition,
                        startOffset,
                        endOffset
                );
                LOG.info(" splits "+split.toString()) ;
                splits.add(split);
            }
        }
        return splits;
    }

}
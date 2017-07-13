package com.xiaoxiaomo.mr.utils.kafka;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * 参考：https://github.com/amient/kafka-hadoop-loader
 *
 *
 * 消费Kafka过程中的快照,提供zk和hdfs两种方案,默认使用zk
 */
public class CheckpointManager {
    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);

    private static final String CONFIG_KAFKA_GROUP_ID = "checkpoints.zookeeper.group.id";
    private static final String CONFIG_CHECKPOINTS_ZOOKEEPER = "checkpoint.use.zookeeper";
    private final FileSystem fs;
    private final Boolean useZkCheckpoints;
    private final KafkaZkUtils zkUtils;
    private final Configuration conf;
    private final Path hdfsCheckpointDir;

    public static void configureUseZooKeeper(Configuration conf, String kafkaGroupId) {
        conf.setBoolean(CONFIG_CHECKPOINTS_ZOOKEEPER, true);
        conf.set(CONFIG_KAFKA_GROUP_ID, kafkaGroupId);
    }

    public CheckpointManager(Configuration conf, KafkaZkUtils zkUtils) throws IOException {
        fs = FileSystem.get(conf);
        this.conf = conf;
        this.zkUtils = zkUtils;
        useZkCheckpoints = conf.getBoolean(CONFIG_CHECKPOINTS_ZOOKEEPER, false);
        hdfsCheckpointDir = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir"), "_OFFSETS");
    }

    /**
     * 获取消费起始点,当无date参数时获得最后一次消费点
     *
     * @param topic     topic
     * @param partition partition
     * @param date      etl date
     * @return offset
     * @throws IOException
     */
    public long getOffsetToConsume(String topic, int partition, String date) throws IOException {
        log.debug("获取消费起始点 getOffsetToConsume useZkCheckpoints:{} ",useZkCheckpoints);
        if (useZkCheckpoints) {
            String consumerGroup = conf.get(CONFIG_KAFKA_GROUP_ID);
            return zkUtils.getLastConsumedOffset(consumerGroup, topic, partition, date);
        } else {
            log.debug("getOffsetToConsume 使用 HDFS 存储 ");
            String comittedCheckpointPath = topic + "-" + partition;
            String pendingCheckpointPath = "_" + topic + "-" + partition;
            if (StringUtils.isNotBlank(date)) {
                comittedCheckpointPath += "-" + date;
                pendingCheckpointPath += "-" + date;
            }
            long nextOffsetToConsume = 0L;
            Path comittedCheckpoint = new Path(hdfsCheckpointDir, comittedCheckpointPath);
            if (fs.exists(comittedCheckpoint)) {
                try (FSDataInputStream in = fs.open(comittedCheckpoint)) {
                    in.readLong();
                    nextOffsetToConsume = in.readLong();
                }
            }
            Path pendingCheckpoint = new Path(hdfsCheckpointDir, pendingCheckpointPath);
            if (fs.exists(pendingCheckpoint)) {
                //TODO 当任务崩溃或者产生竞争时应该锁住
                fs.delete(pendingCheckpoint, true);
            }
            try (FSDataOutputStream out = fs.create(pendingCheckpoint)) {
                out.writeLong(nextOffsetToConsume);
            } catch ( Exception e ){
                e.printStackTrace();
            }
            return nextOffsetToConsume;
        }
    }

    /**
     * 提交本次消费成功点,保存路径为 group/topic/partition,当有date参数时为 group/topic/partition/dt
     *
     * @param topic              topic
     * @param partition          partition
     * @param date               etl date
     * @param lastConsumedOffset offset
     * @throws IOException
     */
    public void commitOffsets(String topic, int partition, String date, long lastConsumedOffset) throws IOException {
        log.debug("commitOffsets对ZK操作 保存本次消费offset:{}, useZkCheckpoints:{} ",lastConsumedOffset,useZkCheckpoints);
        if (useZkCheckpoints) {
            String group = conf.get(CONFIG_KAFKA_GROUP_ID);
            zkUtils.commitLastConsumedOffset(group, topic, partition, date, lastConsumedOffset);
        } else {
            log.debug("getOffsetToConsume 使用 HDFS 存储 ");
            Path pendingCheckpoint = new Path(hdfsCheckpointDir, "_" + topic + "-" + partition);
            Path comittedCheckpoint = new Path(hdfsCheckpointDir, topic + "-" + partition);

            try (FSDataOutputStream out = fs.append(pendingCheckpoint)) {
                out.writeLong(lastConsumedOffset);
            } catch ( Exception e ){
                e.printStackTrace();
            }
            fs.rename(pendingCheckpoint, comittedCheckpoint);
        }
    }
}

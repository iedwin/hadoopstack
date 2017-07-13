package com.xiaoxiaomo.mr.utils.kafka.io;

import com.xiaoxiaomo.mr.utils.kafka.CheckpointManager;
import com.xiaoxiaomo.mr.utils.kafka.KafkaZkUtils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * 参考：https://github.com/amient/kafka-hadoop-loader
 *
 * Des:Kafka记录读取器,使用iterator读取,全部读取后向zk提交
 */
class KafkaInputRecordReader extends RecordReader<MsgMetadataWritable, BytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInputRecordReader.class);
    private static final long EARLIEST_TIME = -2L;
    private static final long LATEST_TIME = -1L;
    final private static String CLIENT_ID = "kafka-hadoop-loader";

    private Configuration conf;
    private KafkaInputSplit split;
    private SimpleConsumer consumer;
    private TopicAndPartition topicAndPartition;

    private String etlDate;
    private long earliestOffset;
    private long latestOffset;
    private long startOffset;
    private long endOffset;
    private long nextOffset;

    private int fetchSize;
    private int timeout;

    private ByteBufferMessageSet messages;
    private Iterator<MessageAndOffset> iterator;
    private MsgMetadataWritable key;
    private BytesWritable value;

    private long numProcessedMessages = 0L;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        initialize(split, context.getConfiguration());
    }

    private void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException {
        LOG.info("Kafka记录读取器RecordReader, 初始化！");
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        this.topicAndPartition = new TopicAndPartition(this.split.getTopic(), this.split.getPartition());
        this.fetchSize = conf.getInt(KafkaInputFormat.CONFIG_KAFKA_MESSAGE_MAX_BYTES, 1024 * 1024);
        this.timeout = conf.getInt(KafkaInputFormat.CONFIG_KAFKA_SOCKET_TIMEOUT_MS, 3000);
        int bufferSize = conf.getInt(KafkaInputFormat.CONFIG_KAFKA_RECEIVE_BUFFER_BYTES, 64 * 1024);

        consumer = new SimpleConsumer(this.split.getBrokerHost(), this.split.getBrokerPort(), timeout, bufferSize, CLIENT_ID);

        //获取实时文件起始和终结点
        this.earliestOffset = getBoundaryOffset(EARLIEST_TIME);
        this.latestOffset = getBoundaryOffset(LATEST_TIME) - 1;
        this.startOffset = this.split.getStartOffset();
        this.endOffset = this.split.getEndOffset() - 1;

        this.etlDate = conf.get(KafkaInputFormat.CONFIG_ETL_DATE);

        //获取reset
        // 1. 如果手动设置了offset参数：KafkaInputFormat.CONFIG_KAFKA_AUTOOFFSET_RESET 即获取参数offset
        // 2. 如果没有设置offset参数：获取checkpoint
        LOG.info("earliestOffset:{} latestOffset:{} startOffset:{} endOffset:{}",earliestOffset,latestOffset,startOffset,endOffset);
        LOG.info("reset:{}",conf.get(KafkaInputFormat.CONFIG_KAFKA_AUTOOFFSET_RESET));
        LOG.info("checkpoint:{}",conf.get("checkpoint"));

        String reset = conf.get(KafkaInputFormat.CONFIG_KAFKA_AUTOOFFSET_RESET, "checkpoint");
        if ("earliest".equals(reset)) {
            resetCheckPoint(earliestOffset,reset);
        } else if ("latest".equals(reset)) {
            resetCheckPoint(latestOffset,reset);
        }

        //最终消费点小于文件起始点,说明部分数据已经过期丢弃,必然有数据丢失
        if (startOffset < earliestOffset) {
            LOG.warn(
                    "initialize Topic: {}, broker: {}, partition: {} ~ Resetting checkpoint as last checkpoint was {} and the earliest available is {}",
                    topicAndPartition.topic(),
                    this.split.getBrokerId(),
                    topicAndPartition.partition(),
                    startOffset,
                    earliestOffset
            );
            resetCheckPoint(earliestOffset,"earliest");
        }

        try (KafkaZkUtils zk = new KafkaZkUtils(
                conf.get(KafkaInputFormat.CONFIG_ZK_CONNECT),
                conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000),
                conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000)
        )) {

            CheckpointManager checkpointManager = new CheckpointManager(conf, zk);
            long checkPoint = checkpointManager.getOffsetToConsume(this.split.getTopic(), this.split.getPartition(), "");
            LOG.info("通过 checkpointManager类 获取offset,即checkPoint {}",checkPoint);
            //如果endOffset<0,有两种情况：
            //1：本次etl任务由调度系统运行,将endOffset设置为latestOffset
            //2：手动执行本日任务，取最后消费提交点为endOffset
            if (endOffset < 0) {
                LOG.info("endOffset < 0 endOffset: {}",endOffset);
                endOffset = (checkPoint < latestOffset && checkPoint > startOffset) ? checkPoint : latestOffset;
            }

            //任务开始时进行存档
            long offset = checkpointManager.getOffsetToConsume(this.split.getTopic(), this.split.getPartition(), etlDate);
            if (offset < 0) {
                checkpointManager.commitOffsets(this.split.getTopic(), this.split.getPartition(), etlDate, startOffset);
            }
        }

        //开始的startOffset 设置为获取数据的起始点nextOffset
        nextOffset = startOffset ;
        LOG.info(
                " initialize Topic: {}, broker: {}, partition: {} ~ Split: {}, earliestOffset: {}, latestOffset: {}, startOffset: {}, endOffset: {}",
                topicAndPartition.topic(),
                this.split.getBrokerId(),
                topicAndPartition.partition(),
                this.split,
                earliestOffset,
                latestOffset,
                startOffset,
                endOffset
        );
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (value == null) {
            value = new BytesWritable();
        }

        //处理messages为空或者队列头的边界情况
        if (messages == null) {
            Map<TopicAndPartition, PartitionFetchInfo> requestInfo = new HashMap<>();

            PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(nextOffset, fetchSize);
            requestInfo.put(topicAndPartition, partitionFetchInfo);

            //通过nextOffset获取Kafka数据
            LOG.debug("通过nextOffset获取Kafka数据,nextOffset:{}",nextOffset);
            FetchRequest fetchRequest = new FetchRequestBuilder()
                    .clientId(CLIENT_ID)
                    .addFetch(topicAndPartition.topic(), topicAndPartition.partition(), nextOffset, fetchSize)
                    .build();
            FetchResponse response = consumer.fetch(fetchRequest);
            messages = response.messageSet(topicAndPartition.topic(), topicAndPartition.partition());

            if (response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()) == ErrorMapping
                    .OffsetOutOfRangeCode()) {
                LOG.info("Out of bounds = " + startOffset);
                return false;
            } else if (response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()) != 0) {
                LOG.warn(
                        "Messages fetch error code: "
                                + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition())
                );
                return false;
            } else {
                iterator = messages.iterator();
                if (!iterator.hasNext()) {
                    return false;
                }
            }
        }

        if (iterator.hasNext() && nextOffset <= endOffset) {
            MessageAndOffset messageOffset = iterator.next();

            LOG.info(
                    "nextKeyValue Topic: {}, broker: {}, partition: {} ~ fetching offset: {}",
                    topicAndPartition.topic(),
                    split.getBrokerId(),
                    topicAndPartition.partition(),
                    nextOffset
            );

            nextOffset = messageOffset.nextOffset();

            Message message = messageOffset.message();

            key = new MsgMetadataWritable(split, messageOffset.offset());

            if (message.isNull()) {
                value.setSize(0);
            } else {
                value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
                LOG.debug("VALUE DATA:{}",new String(value.getBytes()));
            }

            numProcessedMessages++;
            if (!iterator.hasNext()) {
                messages = null;
                iterator = null;
            }
            return true;
        }
        LOG.warn("Unexpected iterator end");
        return false;
    }

    @Override
    public MsgMetadataWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (nextOffset >= endOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (nextOffset - startOffset + 1) / (float) (endOffset - startOffset + 1));
    }

    @Override
    public void close() throws IOException {
        LOG.info(
                "close Topic: {}, broker: {}, partition: {} ~ num. processed messages {}",
                topicAndPartition.topic(),
                split.getBrokerId(),
                topicAndPartition.partition(),
                numProcessedMessages
        );

        //任务完成保存最大消费点
        if (numProcessedMessages > 0) {
            try (KafkaZkUtils zk = new KafkaZkUtils(
                    conf.get(KafkaInputFormat.CONFIG_ZK_CONNECT),
                    conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000),
                    conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000)
            )) {
                CheckpointManager checkpointManager = new CheckpointManager(conf, zk);
                long offset = checkpointManager.getOffsetToConsume(this.split.getTopic(), this.split.getPartition(), "");
                if (offset < latestOffset) {
                    checkpointManager.commitOffsets(split.getTopic(), split.getPartition(), "", latestOffset);
                }
            }
        }
        consumer.close();
    }

    /**
     * 获取边界offset, -2L为起始点,-1L为终结点
     *
     * @param boundary
     * @return
     */
    private long getBoundaryOffset(long boundary) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(boundary, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), CLIENT_ID);

        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
        return offsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
    }

    /**
     * 重新设置startOffset和endOffset
     * 1. reset:earliest , offset=最小值，0设置成-2，之后设置成0，就从最开始的地方--消费到--指定日期
     * 2. reset:latest   , offset=最大值，后面使用end=最大值，从指定日期---消费到--最新数据
     * @param offset
     * @param reset
     */
    private void resetCheckPoint(long offset,String reset) {
        if (offset == 0) {
            offset = EARLIEST_TIME;
        }

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(offset, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), CLIENT_ID);

        LOG.info(
                "getBoundaryOffset Topic: {}, broker: {}, partition: {} ~ Attempting to fetch offset where earliest was {}",
                topicAndPartition.topic(),
                split.getBrokerId(),
                topicAndPartition.partition(),
                offset
        );

        if (offset <= 0) {
            OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
            offset = offsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
        }

        LOG.info(
                "getBoundaryOffset Topic: {}, broker: {}, partition: {} ~ Resetting offset to: {}",
                topicAndPartition.topic(),
                split.getBrokerId(),
                topicAndPartition.partition(),
                offset
        );

        if( "earliest".equals(reset) ){
            startOffset = offset; //offset=0
        }else {
            endOffset = offset; //offset=最大值
        }

    }
}
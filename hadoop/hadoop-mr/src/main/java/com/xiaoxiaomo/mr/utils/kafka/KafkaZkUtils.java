package com.xiaoxiaomo.mr.utils.kafka;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * 参考：https://github.com/amient/kafka-hadoop-loader
 * Des:读取zk的工具类,操作kafka在zk中的存储
 */
public class KafkaZkUtils implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaZkUtils.class);

    private static final String CONSUMERS_PATH = "/consumers";
    private static final String BROKER_IDS_PATH = "/brokers/ids";
    private static final String BROKER_TOPICS_PATH = "/brokers/topics";
    private static final String PARTITIONS = "partitions";

    private final ObjectMapper jsonMapper;

    public final ZkClient client;

    public KafkaZkUtils(String zkConnectString, int sessionTimeout, int connectTimeout) {
        client = new ZkClient(zkConnectString, sessionTimeout, connectTimeout, new StringSerializer());
        jsonMapper = new ObjectMapper(new JsonFactory());
        LOG.info("连接到 Zookeeper ......");
    }

    public String getBrokerName(int brokerId) {
        String data = client.readData(BROKER_IDS_PATH + "/" + brokerId);
        Map<String, Object> map = parseJsonAsMap(data);
        return map.get("host") + ":" + map.get("port");
    }

    /**
     * 寻找partitions leader
     *
     * @param topic topic
     * @return topic各个寻找partitions leader 列表
     */
    public Map<Integer, Integer> getPartitionLeaders(String topic) {
        Map<Integer, Integer> partitionLeaders = new HashMap<>();
        List<String> partitions = getChildrenParentMayNotExist(BROKER_TOPICS_PATH + "/" + topic + "/" + PARTITIONS);
        for (String partition : partitions) {
            String data = client.readData(BROKER_TOPICS_PATH + "/" + topic + "/" + PARTITIONS + "/" + partition + "/state");
            LOG.info("查询到Partitions leader ZK DATA:"+data);
            Map<String, Object> map = parseJsonAsMap(data);

            if (map.containsKey("leader")) {
                partitionLeaders.put(
                        Integer.valueOf(partition),
                        Integer.valueOf(map.get("leader").toString())
                );
            }
        }
        return partitionLeaders;
    }

    /**
     * 获取kafka的zk路径
     *
     * @param group     消费组
     * @param topic     topic
     * @param partition partition
     * @param date      etl date
     * @return path
     */
    private String getOffsetsPath(String group, String topic, int partition, String date) {
        String consumersPath = CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
        return StringUtils.isNotBlank(date) ? consumersPath + "/" + date : consumersPath;
    }

    /**
     * 获取消费点,保存路径 CONSUMERS_PATH + "/" + group/offsets/topic/partition/dt;
     *
     * @param group     消费组
     * @param topic     topic
     * @param partition partition
     * @param date      etl date
     * @return offset
     */
    long getLastConsumedOffset(String group, String topic, int partition, String date) {
        String path = getOffsetsPath(group, topic, partition, date);
        String offset = client.readData(path, true);
        if (offset == null) {
            return -1L;
        }
        LOG.info("OFFSET GET path:{}, offset:{}" , path , offset);
        return Long.valueOf(offset);
    }

    /**
     * 保存消费点,只保存最大值
     *
     * @param group     消费组
     * @param topic     topic
     * @param partition partition
     * @param date      etl date
     * @param offset    offset
     */
    void commitLastConsumedOffset(String group, String topic, int partition, String date, long offset) {
        String path = getOffsetsPath(group, topic, partition, date);

        LOG.info("OFFSET COMMIT path:{}, offset:{}" , path , offset);
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
        client.writeData(path, offset);
    }

    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            LOG.info("PATH "+path);
            return client.getChildren(path);
        } catch (ZkNoNodeException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    private static class StringSerializer implements ZkSerializer {

        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null)
                return null;
            return new String(data);
        }

        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }
    }


    private Map<String, Object> parseJsonAsMap(String data) {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };

        try {
            return jsonMapper.readValue(data, typeRef);
        } catch (IOException e) {
            e.printStackTrace();
            return new HashMap<>();
        }
    }
}

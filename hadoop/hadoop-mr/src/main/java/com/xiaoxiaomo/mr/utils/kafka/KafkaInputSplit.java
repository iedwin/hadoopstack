package com.xiaoxiaomo.mr.utils.kafka;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * Kafka读取分片,封装了kafka的broker/topic/partition,并以startOffset和endOffset判定消费范围
 */
public class KafkaInputSplit extends InputSplit implements Writable {

    private String brokerId;
    private String broker;
    private String topic;
    private int partition;
    private long startOffset;
    private long endOffset;

    public KafkaInputSplit() {
    }

    public KafkaInputSplit(int brokerId, String broker, String topic, int partition, long startOffset, long endOffset) {
        this.brokerId = String.valueOf(brokerId);
        this.broker = broker;
        this.topic = topic;
        this.partition = partition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public void readFields(DataInput in) throws IOException {
        brokerId = Text.readString(in);
        broker = Text.readString(in);
        topic = Text.readString(in);
        partition = in.readInt();
        startOffset = in.readLong();
        endOffset = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, brokerId);
        Text.writeString(out, broker);
        Text.writeString(out, topic);
        out.writeInt(partition);
        out.writeLong(startOffset);
        out.writeLong(endOffset);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{broker};
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getBroker() {
        return broker;
    }

    public String getBrokerHost() {
        String[] hostPort = broker.split(":");
        return hostPort[0];
    }

    public int getBrokerPort() {
        String[] hostPort = broker.split(":");
        return Integer.valueOf(hostPort[1]);
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "KafkaInputSplit{" +
                "broker='" + broker + '\'' +
                ", brokerId='" + brokerId + '\'' +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KafkaInputSplit that = (KafkaInputSplit) o;

        if (partition != that.partition) return false;
        if (startOffset != that.startOffset) return false;
        if (endOffset != that.endOffset) return false;
        if (brokerId != null ? !brokerId.equals(that.brokerId) : that.brokerId != null) return false;
        if (broker != null ? !broker.equals(that.broker) : that.broker != null) return false;
        return topic != null ? topic.equals(that.topic) : that.topic == null;

    }

    @Override
    public int hashCode() {
        int result = brokerId != null ? brokerId.hashCode() : 0;
        result = 31 * result + (broker != null ? broker.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + partition;
        result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
        result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
        return result;
    }
}
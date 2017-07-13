package com.xiaoxiaomo.mr.utils.kafka.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * 参考：https://github.com/amient/kafka-hadoop-loader
 *
 * KafkaInputRecordReader key
 */
public class MsgMetadataWritable implements Writable {

    private Long timestamp;
    private KafkaInputSplit split;
    private long offset;

    public MsgMetadataWritable() {
    }

    public MsgMetadataWritable(KafkaInputSplit split, long offset) {
        this.split = split;
        this.offset = offset;
        this.timestamp = 0L;
    }

    public MsgMetadataWritable(MsgMetadataWritable copyOf, Long timestamp) {
        this.split = copyOf.getSplit();
        this.offset = copyOf.getOffset();
        this.timestamp = timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public KafkaInputSplit getSplit() {
        return split;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        split.write(out);
        WritableUtils.writeVLong(out, getOffset());
        WritableUtils.writeVLong(out, getTimestamp());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        split = new KafkaInputSplit();
        split.readFields(in);
        offset = WritableUtils.readVLong(in);
        timestamp = WritableUtils.readVLong(in);
    }

    @Override
    public String toString() {
        return split.toString() + String.format("Offset: %s", String.valueOf(getOffset()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MsgMetadataWritable)) {
            return false;
        }

        MsgMetadataWritable that = (MsgMetadataWritable) o;

        if (!split.equals(that.split)) {
            return false;
        } else if (offset != that.offset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = split.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}

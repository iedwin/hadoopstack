package com.xiaoxiaomo.mr.utils.kafka;

import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;

/**
 *
 * 可选地,TimestampExtractor可提供配置使输出目录基于分区的时间格式
 */
public interface TimestampExtractor {

    /**
     * 警告:实现这个接口的类对value的操作不要使用value.getBytes(),因为会返回一个长度大于正确值的缓冲字节数组.同seq
     *
     * @param key   与消息关联的元数据
     * @param value 消息负载字节缓冲区
     * @return utc 时间戳
     * @throws IOException
     */
    Long extract(MsgMetadataWritable key, BytesWritable value) throws IOException;
}


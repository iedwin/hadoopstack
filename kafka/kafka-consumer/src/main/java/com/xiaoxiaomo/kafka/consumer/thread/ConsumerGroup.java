package com.xiaoxiaomo.kafka.consumer.thread;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by TangXD on 2017/11/28.
 */
public class ConsumerGroup {

    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; ++i) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList, groupId, topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        for (ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }
    }



    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String groupId = "testGroup1";
        String topic = "test-topic";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }

}

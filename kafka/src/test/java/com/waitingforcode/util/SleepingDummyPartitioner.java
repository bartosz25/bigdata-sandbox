package com.waitingforcode.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class SleepingDummyPartitioner implements Partitioner {

    private static long sleepingTime = 5_000;

    public static void overrideSleepingTime(long newSleepingTime) {
        sleepingTime = newSleepingTime;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);

        System.out.println("------------------------------------------------------");
        System.out.println("Please close your Kafka broker");
        System.out.println("Sleeping for "+sleepingTime+" ms from now");
        try {
            Thread.sleep(sleepingTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Awaken - partition will be returned");

        return partitions.get(0).partition();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

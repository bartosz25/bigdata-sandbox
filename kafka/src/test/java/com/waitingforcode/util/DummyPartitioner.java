package com.waitingforcode.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class DummyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        if (partitions.size() > 1) {
            int keyNr = Integer.valueOf((String) key);
            int partitionNr = keyNr%2 == 0 ? partitions.get(1).partition() : partitions.get(0).partition();
            System.out.println("Message with key '"+key+"' is going to the partition "+partitionNr);
            return partitionNr;
        }
        return partitions.get(0).partition();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

package com.waitingforcode.util;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public class NotifyingRebalanceListener implements ConsumerRebalanceListener {

    private final TopicPartition[] partitionsArray;
    private final CountDownLatch latch;
    private final String consumerName;

    public NotifyingRebalanceListener(String consumerName, TopicPartition[] partitionsArray, CountDownLatch latch) {
        this.consumerName = consumerName;
        this.partitionsArray = partitionsArray;
        this.latch = latch;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Partition revoked from "+consumerName+" = "+partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Partition assigned to "+consumerName+" = "+partitions);
        partitionsArray[1] = partitions.iterator().next();
        latch.countDown();
    }
}

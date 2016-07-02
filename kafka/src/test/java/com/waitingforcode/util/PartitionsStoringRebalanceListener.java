package com.waitingforcode.util;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public class PartitionsStoringRebalanceListener implements ConsumerRebalanceListener  {

    private Collection<TopicPartition> assignedPartitions = new ArrayList<>();
    private final CountDownLatch countDownLatch;

    public PartitionsStoringRebalanceListener(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Partitions after revoking: "+partitions);
        this.assignedPartitions = partitions;
        countDownLatch.countDown();

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Partitions after assignment: "+partitions);
        this.assignedPartitions = partitions;
        countDownLatch.countDown();
    }

    public Collection<TopicPartition> getAssignedPartitions() {
        return assignedPartitions;
    }
}

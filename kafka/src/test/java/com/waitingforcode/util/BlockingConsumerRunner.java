package com.waitingforcode.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class BlockingConsumerRunner<K, V>  implements Runnable {
    private final KafkaConsumer<K, V> consumer;
    private final CountDownLatch latch;
    private final String name;
    private final int blockingTime;
    private final Map<K, V> consumedMessages = new HashMap<>();
    private final Set<Integer> partitionInfos = new HashSet<>();

    public BlockingConsumerRunner(KafkaConsumer<K, V> consumer, CountDownLatch latch, String name, int blockingTime) {
        this.consumer = consumer;
        this.latch = latch;
        this.name = name;
        this.blockingTime = blockingTime;
    }

    @Override
    public void run() {
        try {
            // blocking operations to move offset in
            ConsumerRecords<K, V> records;
            do {
                if (blockingTime > 0) {
                    try {
                        Thread.sleep(blockingTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                records = consumer.poll(100l);
                //System.out.println("a => " + consumer.assignment());
                //System.out.println("Consumer "+name+" got " + records.count());
                for (ConsumerRecord<K, V> record : records) {
                    //System.out.println("Consumer "+name+" got record " + record.key() + " = " + record.value());
                    consumedMessages.put(record.key(), record.value());
                }
                consumer.assignment().forEach(partition -> {
                    partitionInfos.add(partition.partition());
                });
                //System.out.println("assignement is "+consumer.assignment());
               /* consumer.commitSync();    */
            } while (true);//records.isEmpty());
            //latch.countDown();
        } finally {
            consumer.close();
        }
    }

    public Map<K, V> getConsumedMessages() {
        return consumedMessages;
    }

    public Set<Integer> getPartitionInfos() {
        return partitionInfos;
    }
}

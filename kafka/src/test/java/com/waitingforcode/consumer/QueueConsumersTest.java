package com.waitingforcode.consumer;

import com.google.common.base.Joiner;
import com.waitingforcode.Context;
import com.waitingforcode.util.ConsumerHelper;
import com.waitingforcode.util.ProducerHelper;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class QueueConsumersTest {

    private static final String TOPIC = "queuetopic";

    @Test
    public void should_equally_distribute_partitions_consumption_between_consumers() throws IOException, InterruptedException, ExecutionException {
        String testName = "test1_";
        printAlert();

        CountDownLatch latch = new CountDownLatch(2);
        Properties consumerProps = getCommonConsumerProperties("c1");
        KafkaConsumer<String, String> consumer1 =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        Properties consumer2Props = getCommonConsumerProperties("c2");
        KafkaConsumer<String, String> consumer2 =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumer2Props, false, null));

        List<String> consumer1Msg = new ArrayList<>();
        List<String> consumer2Msg = new ArrayList<>();
        List<Integer> consumer1Partitions = new ArrayList<>();
        List<Integer> consumer2Partitions = new ArrayList<>();
        new Thread(new WaitingRunner(consumer1, latch)).start();
        new Thread(new WaitingRunner(consumer2, latch)).start();

        // Give some time to consumers to rebalance
        latch.await(120, TimeUnit.SECONDS);

        // Send messages
        produceMessages(testName);


        CountDownLatch otherLatch = new CountDownLatch(2);
        new Thread(new ResultsAccumulator(consumer1, consumer1Msg, consumer1Partitions, otherLatch)).start();
        new Thread(new ResultsAccumulator(consumer2, consumer2Msg, consumer2Partitions, otherLatch)).start();
        otherLatch.await(120, TimeUnit.SECONDS);

        Collection<String> allConsumed = new ArrayList<>(consumer1Msg);
        allConsumed.addAll(consumer2Msg);

        // Both consumers shouldn't read data from the same partitions
        assertThat(consumer1Msg).isNotEmpty();
        assertThat(consumer2Msg).isNotEmpty();
        assertThat(allConsumed).hasSize(5);
        assertThat(allConsumed).containsOnly("aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee");
        consumer1Msg.forEach(msg -> assertThat(consumer2Msg).doesNotContain(msg));
        // We check here that consumers are linked to different partitions
        // Because of that and the fact that consumerXMsg aren't empty,
        // we can deduce that the messages were put into these two different partitions
        consumer1Partitions.forEach(partition -> assertThat(consumer2Partitions).doesNotContain(partition));
    }

    @Test
    public void should_fail_on_balancing_partitions_when_consumers_are_subscribing_to_topic_in_the_same_thread() throws InterruptedException, IOException, ExecutionException {
        String test = "test2_";
        printAlert();

        Properties consumerProps = getCommonConsumerProperties("c1");
        KafkaConsumer<String, String> consumer1 =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        Properties consumer2Props = getCommonConsumerProperties("c2");
        KafkaConsumer<String, String> consumer2 =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumer2Props, false, null));

        // We poll() in the same thread and it shouldn't provoke rebalancing as in the previous test
        boolean[] assigned1 = new boolean[]{false};
        boolean[] assigned2 = new boolean[]{false};
        consumer1.subscribe(Collections.singletonList(TOPIC), new WaitingRunnerRebalanceListener(assigned1));
        consumer2.subscribe(Collections.singletonList(TOPIC), new WaitingRunnerRebalanceListener(assigned2));

        consumer1.poll(3_000);
        consumer2.poll(3_000);

        while (!assigned1[0] && !assigned1[0]) {
            // do nothing
        }

        // TopicPartition[2] because we suppose that the rebalancing wasn't made
        consumer1.seekToBeginning(consumer1.assignment().toArray(new TopicPartition[2]));
        consumer2.seekToBeginning(consumer2.assignment().toArray(new TopicPartition[2]));

        // Now, produce some messages
        produceMessages(test);

        // This is reading time
        Set<String> messagesConsumer1 = new HashSet<>();
        Set<String> messagesConsumer2 = new HashSet<>();
        for (ConsumerRecord<String, String> record : consumer1.poll(6_000)) {
            messagesConsumer1.add(record.value());
        }
        for (ConsumerRecord<String, String> record : consumer2.poll(6_000)) {
            messagesConsumer2.add(record.value());
        }

        // Here we check if both consumers have the same configuration
        // First, we verify if they're assigned to the same partitions
        // After, we check if consumed messages are the same
        assertThat(consumer1.assignment()).isEqualTo(consumer2.assignment());
        assertThat(messagesConsumer1).isEqualTo(messagesConsumer2);
    }

    @Test
    public void should_not_deliver_messages_in_order_of_insertion() throws InterruptedException, IOException, ExecutionException {
        String testName = "test3_";
        printAlert();

        CountDownLatch latch = new CountDownLatch(2);
        Properties consumerProps = getCommonConsumerProperties("c1");
        KafkaConsumer<String, String> consumer1 =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        Properties consumer2Props = getCommonConsumerProperties("c2");
        KafkaConsumer<String, String> consumer2 =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumer2Props, false, null));

        new Thread(new WaitingRunner(consumer1, latch)).start();
        new Thread(new WaitingRunner(consumer2, latch)).start();

        // Give some time to consumers to rebalance
        latch.await(120, TimeUnit.SECONDS);

        // Send messages
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(testName + "_producer", testName));
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        localProducer.send(new ProducerRecord<>(TOPIC, 0, "A", "a")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, 1, "B", "b")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, 0, "C", "c")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, 1, "D", "d")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, 0, "E", "e")).get();


        Map<Long, String> consumer1Msg = new HashMap<>();
        Map<Long, String> consumer2Msg = new HashMap<>();
        CountDownLatch otherLatch = new CountDownLatch(2);
        new Thread(new SleepingResultAccumulator(consumer1, consumer1Msg, otherLatch, 0)).start();
        new Thread(new SleepingResultAccumulator(consumer2, consumer2Msg, otherLatch, 1_000)).start();
        otherLatch.await(120, TimeUnit.SECONDS);

        Map<Long, String> allConsumed = new TreeMap<>(consumer1Msg);
        allConsumed.putAll(consumer2Msg);

        // Both consumers shouldn't read data from the same partitions
        assertThat(consumer1Msg).isNotEmpty();
        assertThat(consumer2Msg).isNotEmpty();
        assertThat(allConsumed).hasSize(5);
        assertThat(allConsumed.values()).containsOnly("a", "b", "c", "d", "e");
        // check if messages weren't read in order of insertion
        allConsumed.values().stream();
        String consumedMessages = Joiner.on("").appendTo(new StringBuilder(), allConsumed.values()).toString();
        assertThat(consumedMessages).isNotEqualTo("abcde");
        consumer1Msg.forEach((key, value) -> assertThat(consumer2Msg.values()).doesNotContain(value));
    }

    private void produceMessages(String testName) throws ExecutionException, InterruptedException, IOException {
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(testName + "_producer", testName));
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        localProducer.send(new ProducerRecord<>(TOPIC, "A", "aaaaa")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, "B", "bbbbb")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, "C", "ccccc")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, "D", "ddddd")).get();
        localProducer.send(new ProducerRecord<>(TOPIC, "E", "eeeee")).get();
    }


    private void printAlert() throws InterruptedException {
        System.out.println("Before this test, please create a topic with 2 partitions within 10 seconds:");
        System.out.println("bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic queuetopic");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2  --topic queuetopic ");
        Thread.sleep(10_000);
    }

    private Properties getCommonConsumerProperties(String consumerId) throws IOException {
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName("_", consumerId));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        return consumerProps;
    }

    private static class WaitingRunner<K, V> implements Runnable {

        private final KafkaConsumer<K, V> consumer;
        private final CountDownLatch latch;

        public WaitingRunner(KafkaConsumer<K, V> consumer, CountDownLatch latch) {
            this.consumer = consumer;
            this.latch = latch;
        }

        @Override
        public void run() {
            boolean[] assigned = new boolean[]{false};
            consumer.subscribe(Collections.singletonList(TOPIC), new WaitingRunnerRebalanceListener(assigned));
            consumer.poll(500);
            while (!assigned[0]) {
                // do nothing
            }
            consumer.seekToBeginning(consumer.assignment().iterator().next());
            latch.countDown();
        }
    }

    private static class SleepingResultAccumulator<K, V> implements Runnable {

        private final KafkaConsumer<K, V> consumer;
        private final Map<Long, V> messages;
        private final CountDownLatch latch;
        private final long sleepingTime;

        public SleepingResultAccumulator(KafkaConsumer<K, V> consumer, Map<Long, V> messages,
                                         CountDownLatch latch, long sleepingTime) {
            this.consumer = consumer;
            this.messages = messages;
            this.latch = latch;
            this.sleepingTime = sleepingTime;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    int consumed = 0;
                    for (ConsumerRecord<K, V> record : consumer.poll(6_000)) {
                        if (sleepingTime > 0) {
                            try {
                                Thread.sleep(sleepingTime);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        // add 1 to consumption time because of overlapping
                        messages.put(System.currentTimeMillis()+consumed, record.value());
                        consumed++;
                    }
                    latch.countDown();
                }
            } finally {
                consumer.close();
            }
        }
    }

    private static class ResultsAccumulator<K, V> implements Runnable {

        private final KafkaConsumer<K, V> consumer;
        private final Collection<V> messages;
        private final Collection<Integer> partitions;
        private final CountDownLatch latch;

        public ResultsAccumulator(KafkaConsumer<K, V> consumer, Collection<V> messages,
                                  Collection<Integer> partitions, CountDownLatch latch) {
            this.consumer = consumer;
            this.messages = messages;
            this.partitions = partitions;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    for (ConsumerRecord<K, V> record : consumer.poll(6_000)) {
                        messages.add(record.value());
                        partitions.add(record.partition());
                    }
                    latch.countDown();
                }
            } finally {
                consumer.close();
            }
        }
    }

    private static class WaitingRunnerRebalanceListener implements ConsumerRebalanceListener {

        private final boolean[] assigned;

        public WaitingRunnerRebalanceListener(boolean[] assigned) {
            this.assigned = assigned;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assigned[0] = true;
        }
    }

}

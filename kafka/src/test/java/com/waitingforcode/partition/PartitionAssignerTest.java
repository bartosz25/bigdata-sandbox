package com.waitingforcode.partition;


import com.google.common.collect.Lists;
import com.waitingforcode.Context;
import com.waitingforcode.util.ConsumerHelper;
import com.waitingforcode.util.KeyNumberBasedPartitioner;
import com.waitingforcode.util.ProducerHelper;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionAssignerTest {

    private static final String TOPIC_1 = "assignertest1";
    private static final String TOPIC_2 = "assignertest2";

    @Test
    public void should_correctly_assign_partitions_to_consumer_on_range_fashion() throws IOException, InterruptedException {
        String testName = "test1_";
        //printAlert();

        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(testName+"_producer", testName));
        // Send some messages to consume
        sendMessages(producerProps);

        Properties consumerProps = getCommonConsumerProperties("c1", RangeAssignor.class);
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, true, null));
        Properties consumer2Props = getCommonConsumerProperties("c2", RangeAssignor.class);
        KafkaConsumer<String, String> otherConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumer2Props, true, null));;

        // Subscribe and poll from the 1st consumer to guarantee the coordinator creation
        // and metadata request sent
        Map<Long, Collection<TopicPartition>> partitionsInTimeC1 = new TreeMap<>();
        Map<Long, Collection<TopicPartition>> partitionsInTimeC2 = new TreeMap<>();
        new Thread( () -> {
            localConsumer.subscribe(Lists.newArrayList(TOPIC_1, TOPIC_2), new TimeSensibleRebalanceListener(partitionsInTimeC1));
            localConsumer.poll(6_000);
        }).start();
        new Thread( () -> {
            otherConsumer.subscribe(Lists.newArrayList(TOPIC_1, TOPIC_2), new TimeSensibleRebalanceListener(partitionsInTimeC2));
            otherConsumer.poll(6_000);
        }).start();
        // Give some time to consumers to rebalance
        Thread.sleep(60_000);

        // First assumption, since it's range, we expect to have not equal distribution (4 - 2)
        // We expect also that only 1 rebalancing was made, since there are no more events related to group
        // composition
        List<Collection<TopicPartition>> notEmptyPartitionsC1FromStream = partitionsInTimeC1.values().stream()
                .filter(partitions -> !partitions.isEmpty()).collect(Collectors.toList());
        assertThat(notEmptyPartitionsC1FromStream).hasSize(1);
        Collection<TopicPartition> partitionsC1 = notEmptyPartitionsC1FromStream.get(0);
        assertThat(isAsBiggerAs(partitionsC1.size(), 2, 4)).isTrue();
        // Do the same checks for C2
        List<Collection<TopicPartition>> notEmptyPartitionsC2FromStream = partitionsInTimeC2.values().stream()
                .filter(partitions -> !partitions.isEmpty()).collect(Collectors.toList());
        assertThat(notEmptyPartitionsC2FromStream).hasSize(1);
        Collection<TopicPartition> partitionsC2 = notEmptyPartitionsC2FromStream.get(0);
        int previousPartitionsSize = partitionsC1.size();
        assertThat(partitionsC2).hasSize(previousPartitionsSize == 4 ? 2 : 4);
        // Check if there are no partitions consumed twice
        List<String> partitionNames1 = partitionsC1.stream().map(p -> p.topic()+"_"+p.partition()).collect(Collectors.toList());
        List<String> partitionNames2 = partitionsC2.stream().map(p -> p.topic()+"_"+p.partition()).collect(Collectors.toList());
        assertThat(partitionNames1).doesNotContainAnyElementsOf(partitionNames2);
    }

    @Test
    public void should_correctly_assign_partitions_to_consumer_on_round_roubin_fashion() throws IOException, InterruptedException {
        String testName = "test2_";
        //printAlert();

        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(testName+"_producer", testName));
        // Send some messages to consume
        sendMessages(producerProps);

        Properties consumerProps = getCommonConsumerProperties("c1", RoundRobinAssignor.class);
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, true, null));
        Properties consumer2Props = getCommonConsumerProperties("c2", RoundRobinAssignor.class);
        KafkaConsumer<String, String> otherConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumer2Props, true, null));;

        // Subscribe and poll from the 1st consumer to guarantee the coordinator creation
        // and metadata request sent
        Map<Long, Collection<TopicPartition>> partitionsInTimeC1 = new TreeMap<>();
        Map<Long, Collection<TopicPartition>> partitionsInTimeC2 = new TreeMap<>();
        new Thread( () -> {
            localConsumer.subscribe(Lists.newArrayList(TOPIC_1, TOPIC_2), new TimeSensibleRebalanceListener(partitionsInTimeC1));
            localConsumer.poll(6_000);
        }).start();
        new Thread( () -> {
            otherConsumer.subscribe(Lists.newArrayList(TOPIC_1, TOPIC_2), new TimeSensibleRebalanceListener(partitionsInTimeC2));
            otherConsumer.poll(6_000);
        }).start();
        // Give some time to consumers to rebalance
        Thread.sleep(60_000);

        // First assumption, since it's round-robin, we expect to have the same number of partitions
        // assigned for each consumer
        // We expect also that only 1 rebalancing was made, since there are no more events related to group
        // composition
        List<Collection<TopicPartition>> notEmptyPartitionsC1FromStream = partitionsInTimeC1.values().stream()
                .filter(partitions -> !partitions.isEmpty()).collect(Collectors.toList());
        assertThat(notEmptyPartitionsC1FromStream).hasSize(1);
        Collection<TopicPartition> partitionsC1 = notEmptyPartitionsC1FromStream.get(0);
        assertThat(partitionsC1).hasSize(3);
        // Do the same checks for C2
        List<Collection<TopicPartition>> notEmptyPartitionsC2FromStream = partitionsInTimeC2.values().stream()
                .filter(partitions -> !partitions.isEmpty()).collect(Collectors.toList());
        assertThat(notEmptyPartitionsC2FromStream).hasSize(1);
        Collection<TopicPartition> partitionsC2 = notEmptyPartitionsC2FromStream.get(0);
        assertThat(partitionsC2).hasSize(3);
        // Check if there are no partitions consumed twice
        List<String> partitionNames1 = partitionsC1.stream().map(p -> p.topic()+"_"+p.partition()).collect(Collectors.toList());
        List<String> partitionNames2 = partitionsC2.stream().map(p -> p.topic()+"_"+p.partition()).collect(Collectors.toList());
        assertThat(partitionNames1).doesNotContainAnyElementsOf(partitionNames2);
    }

    @Test
    public void should_correctly_assign_key_partitioner_to_producer() throws InterruptedException, IOException, ExecutionException {
        printAlert();
        String testName = "test3_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName("producer_", testName));
        producerProps.setProperty("partitioner.class", KeyNumberBasedPartitioner.class.getCanonicalName());

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        Future<RecordMetadata> recordMetadataFuture1 = localProducer.send(new ProducerRecord<>(TOPIC_1, "1-a", "1a"));
        Future<RecordMetadata> recordMetadataFuture2 = localProducer.send(new ProducerRecord<>(TOPIC_1, "1-b", "1b"));
        Future<RecordMetadata> recordMetadataFuture3 = localProducer.send(new ProducerRecord<>(TOPIC_1, "1-c", "1c"));
        Future<RecordMetadata> recordMetadataFuture4 = localProducer.send(new ProducerRecord<>(TOPIC_1, "2-a", "2a"));
        Future<RecordMetadata> recordMetadataFuture5 = localProducer.send(new ProducerRecord<>(TOPIC_1, "1-d", "1d"));
        Future<RecordMetadata> recordMetadataFuture6 = localProducer.send(new ProducerRecord<>(TOPIC_1, "2-b", "2b"));

        RecordMetadata metadata1 = recordMetadataFuture1.get();
        RecordMetadata metadata2 = recordMetadataFuture2.get();
        RecordMetadata metadata3 = recordMetadataFuture3.get();
        RecordMetadata metadata4 = recordMetadataFuture4.get();
        RecordMetadata metadata5 = recordMetadataFuture5.get();
        RecordMetadata metadata6 = recordMetadataFuture6.get();

        // Checks if partitioner worked correctly
        assertThat(metadata1.partition()).isEqualTo(1);
        assertThat(metadata2.partition()).isEqualTo(1);
        assertThat(metadata3.partition()).isEqualTo(1);
        assertThat(metadata4.partition()).isEqualTo(2);
        assertThat(metadata5.partition()).isEqualTo(1);
        assertThat(metadata6.partition()).isEqualTo(2);
    }

    private boolean isAsBiggerAs(int size, int...values) {
        for (int value : values) {
            if (size == value) {
                return true;
            }
        }
        return false;
    }

    private void printAlert() throws InterruptedException {
        System.out.println("Before launching this test, please create 2 topics within next 10 seconds: ");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3  --topic assignertest1");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3  --topic assignertest2");
        Thread.sleep(10_000);
    }

    private Properties getCommonConsumerProperties(String consumerId, Class<?> assignor) throws IOException {
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName("_", consumerId));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("rebalance.max.retries", "20");
        consumerProps.setProperty("partition.assignment.strategy", assignor.getCanonicalName());
        return consumerProps;
    }

    private void sendMessages(Properties producerProps) {
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        localProducer.send(new ProducerRecord<>(TOPIC_1, "A", "aaaaa"));
        localProducer.send(new ProducerRecord<>(TOPIC_2, "B", "bbbbb"));
        localProducer.send(new ProducerRecord<>(TOPIC_2, "C", "ccccc"));
        localProducer.send(new ProducerRecord<>(TOPIC_2, "D", "ddddd"));
        localProducer.send(new ProducerRecord<>(TOPIC_1, "E", "eeeee"));
        localProducer.flush();
    }

    private static class TimeSensibleRebalanceListener implements ConsumerRebalanceListener {

        private Map<Long, Collection<TopicPartition>> partitionsInTime;

        public TimeSensibleRebalanceListener(Map<Long, Collection<TopicPartition>> partitionsInTime) {
            this.partitionsInTime = partitionsInTime;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            partitionsInTime.put(System.currentTimeMillis(), partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            partitionsInTime.put(System.currentTimeMillis(), partitions);
        }
    }

}

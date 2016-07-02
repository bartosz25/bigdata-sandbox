package com.waitingforcode.cleanup;

import com.waitingforcode.Context;
import com.waitingforcode.util.ConsumerHelper;
import com.waitingforcode.util.ProducerHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class CleanupTest {

    private static final String TOPIC_NAME = "cleanuptopic";

    @Test
    public void should_not_read_deleted_messages() throws IOException, InterruptedException {
        printAlert();
        String testName = "test1_";

        // First, we produce some message fitting to log.retention.bytes
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        KafkaConsumer<String, String> consumer = getConsumer(testName);
        try {
            String msg1 = message("A", 40);
            String msg2 = message("B", 40);
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", msg1));
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, "2", msg2));
            localProducer.flush();

            // After, we add a consumer which should be able to read these messages
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<String, String> records = consumer.poll(5_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records.records(TOPIC_NAME)).extracting("value").containsOnly(msg1, msg2);
            assertThat(records.records(TOPIC_NAME)).extracting("key").containsOnly("1", "2");

            // Next, we add some new message to exceed log.retention.bytes
            // Wait 90 seconds before checking again; this sleep should remove 2 previously added messages because of
            // log.retention.minutes exceeded
            Thread.sleep(90_000);
            String msg3 = message("C", 40);
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, "3", msg3));
            localProducer.flush();

            // And we check once again if the messages from the first add are still available
            consumer.seekToBeginning(consumer.assignment());
            records = consumer.poll(5_000);

            assertThat(records.count()).isEqualTo(1);
            assertThat(records.records(TOPIC_NAME)).extracting("value").containsOnly(msg3);
            assertThat(records.records(TOPIC_NAME)).extracting("key").containsOnly("3");
        } finally {
            consumer.close();
            localProducer.close();
        }
    }

    @Test
    public void should_not_read_messages_prior_to_deletion_message() throws IOException, InterruptedException {
        // First, produce some messages
        String testName = "test2_";
        //fail("Test me with Kafka 0.10.0");

        // First, we produce some message fitting to log.retention.bytes
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        KafkaConsumer<String, String> consumer = getConsumer(testName);
        try {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            consumer.poll(100);
            long currentTime = -1L;
            /*for (int j = 0; j < 10; j++) {
                for (int i = 0; i < 40; i++) {
                    String msg1 = message("A"+i+"_"+j, 40);
                    String msg2 = message("B"+i+"_"+j, 40);
                    localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", msg1));
                    localProducer.send(new ProducerRecord<>(TOPIC_NAME, "2", msg2));
                }
                localProducer.flush();
            }       */
            for (int j = 0; j < 11; j++) {
                currentTime = System.currentTimeMillis();
                for (int i = 0; i < 4; i++) {
                    String msg = i + "_" + currentTime;
                    localProducer.send(new ProducerRecord<>(TOPIC_NAME, "A" + i, msg));
                }
                localProducer.flush();
                Thread.sleep(4000);
            }
            Thread.sleep(8_000);
            // We send another message to be sure that compaction will be triggered
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, null, null));
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, "trigger",
                    "trigger compaction - new write is needed"+currentTime));
            localProducer.flush();
            for (int j = 0; j < 66; j++) {
                for (int i = 0; i < 4; i++) {
                    localProducer.send(new ProducerRecord<>(TOPIC_NAME, j+"B" + i, "xxxxx"));
                }
                localProducer.flush();
            }
            Thread.sleep(25_000);

            // Read them to be sure that they can be deleted further
            //consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            //ConsumerRecords<String, String> records = consumer.poll(5_000);

            //assertThat(records.count()).isEqualTo(2);
            //assertThat(records.records(TOPIC_NAME)).extracting("value").containsOnly(msg1, msg2);
            //assertThat(records.records(TOPIC_NAME)).extracting("key").containsOnly("1", "2");

            // Send new message considered as 'deletion marker' (null key and payload)
            /*for (int i = 0; i < 40; i++) {
                localProducer.send(new ProducerRecord<>(TOPIC_NAME, null), (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            }
            localProducer.flush();*/


            // And we check once again if the messages from the first add are still available
            consumer.seekToBeginning(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
            ConsumerRecords<String, String> records = consumer.poll(5_000);

            records.forEach(r -> System.out.println(r.key()+" / "+r.value()));
            records = consumer.poll(5_000);

            records.forEach(r -> System.out.println(r.key() + " / " + r.value()));
            records = consumer.poll(5_000);

            records.forEach(r -> System.out.println(r.key()+" / "+r.value()));

            assertThat(records.count()).isEqualTo(1);
            assertThat(records.records(TOPIC_NAME)).extracting("value").containsOnly("trigger");
            assertThat(records.records(TOPIC_NAME)).extracting("key").containsOnly("trigger compaction - new write is needed"+currentTime);
        } finally {
            consumer.close();
            localProducer.close();
        }
    }

    @Test
    public void should_compact_duplicated_messages() throws InterruptedException, IOException {
        printAlert();
        String testName = "test3_";

        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, true, null));
        long currentTime = 0L;
        try {
            // First, we produce 44 messages with the same keys but different content
            // We expected that only messages from last run be stored after
            // compaction
            for (int j = 0; j < 11; j++) {
                currentTime = System.currentTimeMillis();
                for (int i = 0; i < 4; i++) {
                    String msg = i + "_" + currentTime;
                    localProducer.send(new ProducerRecord<>(TOPIC_NAME, "A" + i, msg));
                }
                localProducer.flush();
            }
            // wait 2 seconds which is the time corresponding to making
            // active log segment inactive
            Thread.sleep(2_000);
            // We send another message to be sure that compaction will be triggered;
            // it creates new active segment
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, "trigger",
                    "trigger compaction - new write is needed" + currentTime));
            localProducer.flush();

            // Give some time to compaction to finish
            Thread.sleep(45_000);

            // Subscribe to topic and get all record from the beginning of the partition
            // We should only be able to get messages coming from the last run
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            consumer.poll(3_000);
            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, String> records = null;

            List<String> receivedMsg = new ArrayList<>();
            List<String> receivedKeys = new ArrayList<>();
            while (records == null || !records.isEmpty()) {
                records = consumer.poll(5_000);
                for (ConsumerRecord<String, String> r : records) {
                    receivedKeys.add(r.key());
                    receivedMsg.add(r.value());
                }
                Thread.sleep(4_000);
                consumer.commitSync();
            }

            // Even if we seek to the beginning, we should always get the record put by
            // the last (10th) loop execution
            // It's because of compaction is done and can be checked in log-cleaner.log:
            // INFO [kafka-log-cleaner-thread-0],
            // Log cleaner thread 0 cleaned log cleanuptopic-0 (dirty section = [4, 44])
            // 0,0 MB of log processed in 0,0 seconds (0,1 MB/sec).
            //     Indexed 0,0 MB in 0,0 seconds (0,4 Mb/sec, 12,5% of total time)
            // Buffer utilization: 0,0%
            //     Cleaned 0,0 MB in 0,0 seconds (0,1 Mb/sec, 87,5% of total time)
            // Start size: 0,0 MB (44 messages)
            // End size: 0,0 MB (4 messages)
            // 90,9% size reduction (90,9% fewer messages)
            // (kafka.log.LogCleaner)
            assertThat(receivedMsg).hasSize(5);
            assertThat(receivedKeys).containsOnly("A0", "A1", "A2", "A3", "trigger");
            for (String msg : receivedMsg) {
                assertThat(msg).contains(""+currentTime);
            }
        } finally {
            consumer.close();
            localProducer.close();
        }
    }

    private String message(String letter, int repetitions) {
        String message = "";
        for (int i = 0; i < repetitions; i++) {
            message += letter;
        }
        return message;
    }

    private KafkaConsumer<String, String> getConsumer(String testName) throws IOException {
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, true, null));
    }

    private void printAlert() throws InterruptedException {
        System.out.println("Before this test, please create a topic with 2 partitions within 10 seconds:");
        System.out.println("bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic cleanuptopic");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1  --topic cleanuptopic");
        Thread.sleep(10_000);
    }


}

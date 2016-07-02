package com.waitingforcode.producer;


import com.waitingforcode.Context;
import com.waitingforcode.util.ConsumerHelper;
import com.waitingforcode.util.DummyPartitioner;
import com.waitingforcode.util.ProducerHelper;
import com.waitingforcode.util.SleepingDummyPartitioner;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class BasicProducerTest {

    private static final String TOPIC_NAME = "basicproducer";

    private boolean replicated = true;

    @Test
    public void should_correctly_send_message() throws IOException, InterruptedException {
        printAdvice(!replicated);
        Thread.sleep(10_000);
        String testName = "test1_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        try {
            String key1 = "1" + System.currentTimeMillis(), key2 = "2" + System.currentTimeMillis();
            String value1 = "A" + System.currentTimeMillis(), value2 = "B" + System.currentTimeMillis();
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            localProducer.flush();

            KafkaConsumer<String, String> consumer = getConsumer(testName);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<String, String> records = consumer.poll(15_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").contains(value1, value2);
        } finally {
            localProducer.close();
        }
    }

    @Test
    public void should__flush_message_immediately_without_explicit_flush_call() throws IOException, InterruptedException {
        printAdvice(!replicated);
        Thread.sleep(10_000);
        String testName = "test2_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        // max.request.size	 corresponds approximately to the total size of both sent messages
        // In fact, their real size is 16384 and since they're bigger than the configured limit
        // You should be able to see that in the logs:
        // TRACE Sending record ProducerRecord(topic=basicproducer, partition=null, key=11464525973455, value=A1464525973455,
        //       timestamp=null) with callback null to topic basicproducer partition 0
        // TRACE Allocating a new 16384 byte message buffer for topic basicproducer partition 0
        // TRACE Waking up the sender since topic basicproducer partition 0 is either full or getting a new batch
        // TRACE Sending record ProducerRecord(topic=basicproducer, partition=null, key=21464525973455,
        //       value=B1464525973456, timestamp=null) with callback null to topic basicproducer partition 0

        producerProps.setProperty("max.request.size", "15384");
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        try {
            String key1 = "1" + System.currentTimeMillis(), key2 = "2" + System.currentTimeMillis();
            String value1 = "A" + System.currentTimeMillis(), value2 = "B" + System.currentTimeMillis();
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            Thread.sleep(2000);

            KafkaConsumer<String, String> consumer = getConsumer(testName);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<String, String> records = consumer.poll(5_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").contains(value1, value2);
        } finally {
            localProducer.close();
        }
    }

    @Test
    public void should_correctly_get_information_about_flushed_messages_from_callback() throws IOException, InterruptedException {
        printAdvice(replicated);
        Thread.sleep(10_000);
        String testName = "test3_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        Map<String, String> stats = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(1);
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "key_1", "value_1"), (metadata, exception) -> {
            stats.put("offset", ""+metadata.offset());
            stats.put("topic", metadata.topic());
            stats.put("partition", ""+metadata.partition());
            latch.countDown();
        });
        latch.await(2, TimeUnit.SECONDS);

        System.out.println("stats are "+stats);
        assertThat(stats).hasSize(3);
        assertThat(stats.get("topic")).isEqualTo(TOPIC_NAME);
        assertThat(stats.get("partition")).isEqualTo("0");
        assertThat(stats.get("offset")).isEqualTo("0");
    }

    @Test
    public void should_correctly_produce_message_to_second_partition() throws IOException, InterruptedException {
        printAdvice(replicated);
        Thread.sleep(10_000);
        String testName = "test4_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty("partitioner.class", DummyPartitioner.class.getCanonicalName());
        System.out.println("> "+producerProps);
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        Map<String, String> statsB = new HashMap<>();
        Map<String, String> statsC = new HashMap<>();
        Map<String, String> statsD = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(3);
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "2", "B"),
                (metadata, exception) -> handleSentMessageMetadata(metadata, statsB, latch));
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "3", "C"),
                (metadata, exception) -> handleSentMessageMetadata(metadata, statsC, latch));
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "4", "D"),
                (metadata, exception) -> handleSentMessageMetadata(metadata, statsD, latch));
        latch.await(2, TimeUnit.SECONDS);

        assertThat(statsB).hasSize(3);
        assertThat(statsB.get("topic")).isEqualTo(TOPIC_NAME);
        assertThat(statsB.get("partition")).isEqualTo("0");
        assertThat(statsB.get("offset")).isEqualTo("0");
        assertThat(statsC).hasSize(3);
        assertThat(statsC.get("topic")).isEqualTo(TOPIC_NAME);
        assertThat(statsC.get("partition")).isEqualTo("1");
        assertThat(statsC.get("offset")).isEqualTo("0");
        assertThat(statsC).hasSize(3);
        assertThat(statsD.get("topic")).isEqualTo(TOPIC_NAME);
        assertThat(statsD.get("partition")).isEqualTo("0");
        assertThat(statsD.get("offset")).isEqualTo("1");
    }

    @Test
    public void should_not_fail_when_messages_was_not_replicated_enough() throws InterruptedException, IOException {
        printAdvice(replicated);
        System.out.println("Now, turn down one broker to see if the message will be delivered");
        Thread.sleep(10_000);
        String testName = "test5_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        // Even if 'all' level is expected, in-sync replicas will contain only alive broker
        // and it will expects only its ack
        // Broker previously turned down will catch up missing messages after rstart
        producerProps.setProperty("acks", "all");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "test5_key", "test5_value"));
        localProducer.flush();

        KafkaConsumer<String, String> consumer = getConsumer(testName);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        ConsumerRecords<String, String> records = consumer.poll(5_000);

        assertThat(records.count()).isEqualTo(1);
    }

    @Test
    public void should_correctly_send_message_taking_care_of_delivery() throws IOException, InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        printAdvice(replicated);
        Thread.sleep(10_000);
        String testName = "test6_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        Future<RecordMetadata> messageFuture = localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "A"));
        RecordMetadata messageMetadata = messageFuture.get(5, TimeUnit.SECONDS);

        KafkaConsumer<String, String> consumer = getConsumer(testName);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        ConsumerRecords<String, String> records = consumer.poll(5_000);

        assertThat(records.count()).isEqualTo(1);
        assertThat(messageMetadata).isNotNull();
        assertThat(messageMetadata.topic()).isEqualToIgnoringCase(TOPIC_NAME);
    }

    @Test
    public void should_not_fail_on_compressing_message_with_different_compression_than_topic_and_broker() throws InterruptedException, IOException {
        /**
         * For this test, we expect to create a topic with snappy compression, run broker with lz4 compression
         * and send the message with gzip compression.
         *
         * Not only the message will be correctly delivered but also will be correctly uncompressed by consumer.
         *
         * To run broker with specific compression, add compression.type=lz4 to the configuration file.
         * To create a topic with snappy compression we can use this command:
         * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2
         *                      --partitions 2 --topic basicproducer --config compression.type=snappy
         * After executing the last command, check if the compression was correctly defined by calling:
         * bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic basicproducer
         *
         * Expected output is:
         * Topic:basicproducer	PartitionCount:2	ReplicationFactor:2	Configs:compression.type=snappy
         * Topic: basicproducer	Partition: 0	Leader: 0	Replicas: 0,1	Isr: 0,1
         * Topic: basicproducer	Partition: 1	Leader: 1	Replicas: 1,0	Isr: 1,0
         *
         * Broker's compression is expected to be gzip.
         */
        System.out.println("Please delete and recreate a topic with snappy compression");
        Thread.sleep(10_000);
        String testName = "test7_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty("compression.type", "gzip");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "key_1", "payload_A"));

        /**
         * The test shouldn't fail because compression defined at broker level is only the
         * default compression used on topics not specifying one. In consequence, if take a look
         * at .log files storing compressed data, we'll see a kind of binary data prefixed with
         * SNAPPY word, representing the compression format:
         * SNAPPY^@^@^@^@^A^@^@^@^A^@^@^@+0^@^@^Y^A<90>$#<E1>.
         * <CE>^A^@^@^@^AT<FC><A9>8^R^@^@^@^Ekey_1^@^@^@     payload_A
         */
        KafkaConsumer<String, String> consumer = getConsumer(testName);
        try {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<String, String> records = consumer.poll(5_000);

            assertThat(records.count()).isEqualTo(1);
            assertThat(records.records(TOPIC_NAME)).extracting("key").containsOnly("key_1");
            assertThat(records.records(TOPIC_NAME)).extracting("value").containsOnly("payload_A");
        } finally {
            consumer.close();
            localProducer.close();
        }
    }

    @Test
    public void should_handle_too_big_message_in_listener() throws InterruptedException, IOException, TimeoutException, ExecutionException {
        printAdvice(replicated);
        Thread.sleep(10_000);
        String testName = "test8_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "1");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        /**
         * send() method never throws an exception. To handle it, we need to register appropriated
         * callback. To see that, compare the result of this test with the result of the next test.
         *
         * Thrown exception will have message similar to:
         * org.apache.kafka.common.errors.RecordTooLargeException: The message is 29 bytes when serialized
         * which is larger than the total memory buffer you have configured with the buffer.memory configuration.
         */
        List<RecordTooLargeException> exceptions = new ArrayList<>();
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "BB"), (metadata, exception) ->
                exceptions.add((RecordTooLargeException) exception));

        KafkaConsumer<String, String> consumer = getConsumer(testName);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        ConsumerRecords<String, String> records = consumer.poll(5_000);

        assertThat(records.count()).isEqualTo(0);
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).getMessage())
                .contains("The message is 37 bytes when serialized which is larger than the total memory " +
                        "buffer you have configured with the buffer.memory configuration");
    }

    @Test
    public void should_correctly_listen_to_message_flush_when_batch_is_full() throws IOException, InterruptedException {
        printAdvice(!replicated);
        Thread.sleep(10_000);
        String testName = "test9_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        // For Kafka 0.9.1, the size of each message was 29. Since 0.10.0, because of timestamp add,
        // this value is now 37 bytes
        producerProps.setProperty("batch.size", 37 * 3 + "");

        TestAppender testAppender = new TestAppender(); //create appender
        String PATTERN = "%m";
        testAppender.setLayout(new PatternLayout(PATTERN));
        testAppender.setThreshold(Level.TRACE);
        testAppender.activateOptions();
        Logger.getRootLogger().addAppender(testAppender);

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "AA")); // 37 bytes
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "2", "BB"));
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "3", "CC"));
        Thread.sleep(2000);
        /**
         * Messages are sent through special sender Thread. It's awaken every time when:
         * - batch is full
         * - new batch is created
         *
         * These events are handled by {@link RecordAccumulator}
         *
         * To analyze that we'll looking for log.trace("") call with appropriated text through previously registered
         * TestAppender class.
         */

        // batch should be sent exactly twice
        // 1) Because new batch was created
        // 2) Because previously created batch was filled up
        assertThat(testAppender.getMessagesCounter()).isEqualTo(2);
    }

    @Test
    public void should_not_block_when_broker_goes_down_after_reaching_request_timeout_value() throws InterruptedException, IOException {
        printAdvice(!replicated);
        SleepingDummyPartitioner.overrideSleepingTime(30_000L);
        Thread.sleep(10_000);
        String testName = "test10_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty("retries", "5");
        producerProps.setProperty("partitioner.class", SleepingDummyPartitioner.class.getCanonicalName());
        // This property determines how long main thread will wait for requests processing
        // It can be useful in our situation when broker goes down after successful connection
        producerProps.setProperty("request.timeout.ms", "3000");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        System.out.println("Now, turn down one broker to see if the message will be delivered");

        // Callback is not expected to be called because the connection
        // was lost and the main worry of client was to reconnect and not
        // to resend the message
        List<TimeoutException> exceptions = new ArrayList<>();
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "A"),
                (metadata, exception) -> {
                    exceptions.add((TimeoutException) exception);
                });
        long startTime = System.currentTimeMillis();
        localProducer.flush();

        // 3 seconds should elapse
        assertThat((System.currentTimeMillis() - startTime) / 1000).isEqualTo(3);
        // Logs are expected to be printed only once:
        // TRACE Produced messages to topic-partition basicproducer-0 with base offset offset -1 and error: {}.
        //       (org.apache.kafka.clients.producer.internals.RecordBatch:92)
        // TRACE org.apache.kafka.common.errors.TimeoutException: Batch containing 1 record(s) expired due to timeout while
        //       requesting metadata from brokers for basicproducer-0
        // TRACE Expired 1 batches in accumulator (org.apache.kafka.clients.producer.internals.RecordAccumulator:249)

        assertThat(exceptions).hasSize(1);
    }

    private static final class TestAppender extends ConsoleAppender {

        private static final String EXPECTED_BATCH_MSG_1 =
                "Waking up the sender since topic basicproducer partition ";
        private static final String EXPECTED_BATCH_MSG_2 = "is either full or getting a new batch";
        private int messagesCounter = 0;

        @Override
        public void append(LoggingEvent event) {
            if (event.getRenderedMessage().contains(EXPECTED_BATCH_MSG_1) &&
                    event.getRenderedMessage().contains(EXPECTED_BATCH_MSG_2)) {
                messagesCounter++;
            }
        }

        public int getMessagesCounter() {
            return messagesCounter;
        }
    }

    private void handleSentMessageMetadata(RecordMetadata metadata, Map<String, String> statsHolder, CountDownLatch latch) {
        statsHolder.put("offset", ""+metadata.offset());
        statsHolder.put("topic", metadata.topic());
        statsHolder.put("partition", ""+metadata.partition());
        latch.countDown();
    }

    private KafkaConsumer<String, String> getConsumer(String testName) throws IOException {
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
    }


    private void printAdvice(boolean replicated) {
        System.out.println("Before launching this test, think about recreate a topic: ");
        System.out.println("bin/kafka-topics.sh --delete --zookeeper localhost:2181  --topic basicproducer");
        if (replicated) {
            System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic basicproducer");
        } else {
            System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic basicproducer");
        }
    }


    // TODO -> write a question about compression consistency
    // TODO -> write about a potential bug when connection is lost
    // TODO -> callback n'est pas appelé quand la connection est perdue au milieu
}

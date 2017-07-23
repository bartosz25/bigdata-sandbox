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
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Fail.fail;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class BasicProducerTest {

    private static final String TOPIC_NAME = "basicproducer";

    @Test
    public void should_correctly_send_message() throws IOException {
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
            ConsumerRecords<String, String> records = consumer.poll(5_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").contains(value1, value2);
        } finally {
            localProducer.close();
        }
    }

    @Test
    public void should__flush_message_immediately_without_explicit_flush_call() throws IOException, InterruptedException {
        String testName = "test2_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        // set small time before automatic flush
        // By doing so we can try to consume messages within the sleep() of this time
        producerProps.setProperty("max.request.size", "1");
        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        try {
            String key1 = "1" + System.currentTimeMillis(), key2 = "2" + System.currentTimeMillis();
            String value1 = "A" + System.currentTimeMillis(), value2 = "B" + System.currentTimeMillis();
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            localProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            //Thread.sleep(2000);

            KafkaConsumer<String, String> consumer = getConsumer(testName);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<String, String> records = consumer.poll(5_000);

            fail("FAILING TEST - FIX OR REMOVE");

            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").contains(value1, value2);
        } finally {
            localProducer.close();
        }
    }

    @Test
    public void should_correctly_get_information_about_flushed_messages_from_callback() throws IOException, InterruptedException {
        printAdvice();
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
        printAdvice();
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
    public void should_fail_when_messages_was_not_replicated_enough() throws InterruptedException, IOException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test5_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty("acks", "all");
        //producerProps.setProperty("timeout.ms", "30000");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "test5_key", "test5_value"));
        System.out.println("Now, turn down one broker to see if the message will be delivered");
        Thread.sleep(10_000);
        localProducer.flush();

        KafkaConsumer<String, String> consumer = getConsumer(testName);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        ConsumerRecords<String, String> records = consumer.poll(5_000);

        assertThat(records.count()).isEqualTo(0);
        fail("This test doesn't work either !");
    }

    @Test
    public void should_correctly_send_message_taking_care_of_delivery() throws IOException, InterruptedException, TimeoutException, ExecutionException {
        printAdvice();
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
         * kbin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic basicproducer
         *
         * Expected output is:
         * Topic:basicproducer	PartitionCount:2	ReplicationFactor:2	Configs:compression.type=snappy
         * Topic: basicproducer	Partition: 0	Leader: 0	Replicas: 0,1	Isr: 0,1
         * Topic: basicproducer	Partition: 1	Leader: 1	Replicas: 1,0	Isr: 1,0
         */
        System.out.println("Please delete and recreate a topic with snappy compression");
        Thread.sleep(10_000);
        // TODO : sprobuj wykonac zapytania automatycznie z poziomu Javy
        String testName = "test7_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        // TODO : change all properties to constants
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "A"));

        /**
         * We don't need to specify compression type in consumer side - the compression is handled
         * by broker thanks to configuration entry "compression.type" which default is "producer".
         * This default value means that compression used by broker will be the same as the
         * compression used by producer.
         */
        // TODO : wyjasnic to lepiej po dodaniu wiadomosci na forum
        KafkaConsumer<String, String> consumer = getConsumer(testName);
        try {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<String, String> records = consumer.poll(5_000);

            assertThat(records.count()).isEqualTo(1);
            assertThat(records.records(TOPIC_NAME)).extracting("key").containsOnly("1");
            assertThat(records.records(TOPIC_NAME)).extracting("value").containsOnly("A");
        } finally {
            consumer.close();
            localProducer.close();
        }
    }

    @Test
    public void should_handle_too_big_message_in_listener() throws InterruptedException, IOException, TimeoutException, ExecutionException {
        printAdvice();
        //Thread.sleep(10_000);
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
                .contains("The message is 29 bytes when serialized which is larger than the total memory buffer you have configured");
    }

    @Test
    public void should_not_throw_exception_when_message_is_too_big() throws IOException {
        // The scenario is exactly the same as in the previous test. The only difference is that
        // we allow sent message to fail silently
        printAdvice();
        //Thread.sleep(10_000);
        String testName = "test9_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "1");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "BB"));

        KafkaConsumer<String, String> consumer = getConsumer(testName);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        ConsumerRecords<String, String> records = consumer.poll(5_000);

        assertThat(records.count()).isEqualTo(0);
    }

    @Test
    public void should_correctly_listen_to_message_flush_when_batch_is_full() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test10_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, 29*3+"");

        TestAppender testAppender = new TestAppender(); //create appender
        String PATTERN = "%m";
        testAppender.setLayout(new PatternLayout(PATTERN));
        testAppender.setThreshold(Level.TRACE);
        testAppender.activateOptions();
        Logger.getRootLogger().addAppender(testAppender);

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "AA")); // 29 bytes
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "2", "BB"));
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "3", "CC"));

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
        printAdvice();
        SleepingDummyPartitioner.overrideSleepingTime(30_000L);
        Thread.sleep(10_000);
        String testName = "test11_";
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        producerProps.setProperty("retries", "5");
        producerProps.setProperty("partitioner.class", SleepingDummyPartitioner.class.getCanonicalName());
        // This property determines how long main thread will wait for requests processing
        // It can be useful in our situation when broker goes down after successful connection
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");

        KafkaProducer<String, String> localProducer =
                new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));

        System.out.println("Now, turn down one broker to see if the message will be delivered");
        // Callback is not expected to be called
        // TODO : see that in source code
        List<TimeoutException> exceptions = new ArrayList<>();
        localProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "A"),
                (metadata, exception) -> {
                    exceptions.add((TimeoutException) exception);
                });
        long startTime = System.currentTimeMillis();
        localProducer.flush();

        // 3 seconds should elapse
        assertThat((System.currentTimeMillis()-startTime)/1000).isEqualTo(3);
        Thread.sleep(2_000);
        assertThat(exceptions).isEmpty();
    }

    private static final class TestAppender extends ConsoleAppender {

        private static final String EXPECTED_BATCH_MSG =
                "Waking up the sender since topic basicproducer partition 0 is either full or getting a new batch";
        private int messagesCounter = 0;

        @Override
        public void append(LoggingEvent event) {
            if (event.getRenderedMessage().contains(EXPECTED_BATCH_MSG)) {
                messagesCounter++;
            }
        }

        public int getMessagesCounter() {
            return messagesCounter;
        }
    }

    @Test
    public void should_see_new_partition_available() {
        // TODO : test bedzie dotyczyl zapytania metadata -> metadata.max.age.ms
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


    private void printAdvice() {
        System.out.println("Before launching this test, think about recreate a topic: ");
        System.out.println("bin/kafka-topics.sh --delete --zookeeper localhost:2181  --topic basicproducer");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic basicproducer");
    }


    // TODO -> write a question about compression consistency
    // TODO -> write about a potential bug when connection is lost
    // TODO -> callback n'est pas appelé quand la connection est perdue au milieu
}

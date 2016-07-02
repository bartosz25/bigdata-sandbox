package com.waitingforcode.consumer;


import com.waitingforcode.Context;
import com.waitingforcode.util.ConsumerHelper;
import com.waitingforcode.util.PartitionsStoringRebalanceListener;
import com.waitingforcode.util.ProducerHelper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/**
 * Test cases showing the way of working for single consumer.
 */
public class SingleConsumerTest {

    private static final String TOPIC_NAME = "singleconsumertopic";

    private Producer<String, String> producer;

    @Before
    public void initContext() throws IOException {
        producer = new KafkaProducer<>(ProducerHelper.decorateWithDefaults(Context.getInstance().getCommonProperties()));
    }

    @After
    public void shutdown() {
        producer.close();
    }

    @Test
    public void should_correctly_subscribe_even_if_kafka_broker_is_down() throws IOException, InterruptedException {
        // Consumers subscribe() to Kafka topic even if the broker is down
        // Subscribing is lazy evaluated and is effectively done only when poll() is invoked
        System.out.println("Turn off your Kafka broker now - you've 10 seconds from now to do that");
        Thread.sleep(10_000);
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, "test1_"));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        assertThat(localConsumer.subscription()).hasSize(1);
        assertThat(localConsumer.subscription()).containsOnly(TOPIC_NAME);
    }

    @Test
    public void should_correctly_connect_to_partition_even_if_broker_is_down() throws InterruptedException, IOException {
        System.out.println("Turn off your Kafka broker now - you've 10 seconds from now to do that");
        Thread.sleep(10_000);
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, "test2_"));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        // Even if broker is down, consumer correctly assign partition
        // and do not block, until poll() call for lazy evaluation
        TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
        localConsumer.assign(Collections.singletonList(partition0));
        localConsumer.seekToBeginning(Collections.singletonList(partition0));

        TopicPartition assignedPartition = localConsumer.assignment().iterator().next();
        assertThat(assignedPartition.partition()).isEqualTo(0);
        assertThat(assignedPartition.topic()).isEqualTo(TOPIC_NAME);
    }

    @Test
    public void should_correctly_poll_messages() throws InterruptedException, IOException, KeeperException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test3_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        // earliest - we want that the consumer gets all messages of given topic, from the beginning when
        // there is no offset specified
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        // It is also possible for the consumer to manually
        // specify the partitions that are assigned to it through assign(List), which disables this dynamic partition assignment.
        localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        try {
            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            // Send messages
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, "1", "A"));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, "2", "B"));

            // Wait for messages being flushed to the broker
            sampleProducer.flush();

            // Get previously flushed messages
            // Be sure that the topic is empty before testing. Since we get the earliest messages
            // we could risk to retrieve more than the 2 first
            ConsumerRecords<String, String> records = localConsumer.poll(5_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records.records(TOPIC_NAME)).extracting("key").containsOnly("1", "2");
            assertThat(records.records(TOPIC_NAME)).extracting("value").containsOnly("A", "B");
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_correctly_assign_a_partition_to_consumer() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test4_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));

        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            PartitionsStoringRebalanceListener rebalanceListener = new PartitionsStoringRebalanceListener(countDownLatch);
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME), rebalanceListener);
            countDownLatch.await(4, TimeUnit.SECONDS);
            // As already proved, partitions assignment is lazy,
            // only when poll() is not invoked, partitions are not assigned
            assertThat(rebalanceListener.getAssignedPartitions()).isEmpty();

            // Let's invoke poll() to force assignment
            localConsumer.poll(3_000);

            assertThat(rebalanceListener.getAssignedPartitions()).hasSize(1);
            assertThat(rebalanceListener.getAssignedPartitions()).extracting("partition").containsOnly(0);
            assertThat(rebalanceListener.getAssignedPartitions()).extracting("topic").containsOnly(TOPIC_NAME);
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_read_the_same_records_twice_because_of_not_committed_offset() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test5_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));

        try {
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            // Produce some sample messages
            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            String key1 = "1"+ System.currentTimeMillis(), key2 = "2"+System.currentTimeMillis();
            String value1 = "A"+System.currentTimeMillis(), value2 = "B"+ System.currentTimeMillis();
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            sampleProducer.flush();

            // poll for the first time
            ConsumerRecords<String, String> records = localConsumer.poll(3_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").containsOnly(value1, value2);

            // Consumer keeps localy last consumed offset, even if it doesn't commit
            // We can see that below and it's the reason why after next 2 assertions
            // we reopen the consumer to make a test
            TopicPartition assignedPartition = new TopicPartition(TOPIC_NAME, 0);
            long currentPosition = localConsumer.position(assignedPartition);
            OffsetAndMetadata commitedPosition = localConsumer.committed(assignedPartition);
            assertThat(currentPosition).isEqualTo(2L);
            assertThat(commitedPosition).isNull(); // = no prior commit done

            // No offset was committed, poll() once again without producing new messages
            // We expect to get the same messages
            // To illustrate this desynchronization, we reinitialize our consumer
            localConsumer.close();
            localConsumer =
                    new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            // Check if polled() records are the same
            records = localConsumer.poll(3_000);
            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").containsOnly(value1, value2);
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_read_records_only_once_since_offset_is_committed_manually_before_consumer_reinitialization() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test6_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));

        try {
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            // Produce some sample messages
            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            String key1 = "1"+ System.currentTimeMillis(), key2 = "2"+System.currentTimeMillis();
            String value1 = "A"+System.currentTimeMillis(), value2 = "B"+ System.currentTimeMillis();
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            sampleProducer.flush();

            // poll for the first time
            ConsumerRecords<String, String> records = localConsumer.poll(3_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").containsOnly(value1, value2);

            // This time we commit offset before closing previous consumer. It's expected
            // that it starts to consumer only new messages and because there are no
            // new ones, it should consume nothing
            localConsumer.commitSync();

            TopicPartition assignedPartition = new TopicPartition(TOPIC_NAME, 0);
            long currentPosition = localConsumer.position(assignedPartition);
            OffsetAndMetadata commitedPosition = localConsumer.committed(assignedPartition);
            assertThat(currentPosition).isEqualTo(2L);
            assertThat(commitedPosition.offset()).isEqualTo(2L);

            // Reinitialize consumer
            localConsumer.close();
            localConsumer =
                    new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            records = localConsumer.poll(3_000);
            assertThat(records.count()).isEqualTo(0);
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_correctly_read_one_record_once_and_at_most_once() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test7_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));

        long alreadyConsumedOffset;
        try {
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            // Produce some sample messages
            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            String key1 = "1"+ System.currentTimeMillis(), key2 = "2"+System.currentTimeMillis();
            String value1 = "A"+System.currentTimeMillis(), value2 = "B"+ System.currentTimeMillis();
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            sampleProducer.flush();

            // poll for the first time
            ConsumerRecords<String, String> records = localConsumer.poll(3_000);

            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").containsOnly(value1, value2);

            TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
            alreadyConsumedOffset = localConsumer.position(partition0);

            // Expressly we do not commit offset. But even with that, we won't consume
            // messages twice because of "external" synchronization
            // "mechanism" (alreadyConsumedOffset var) and seek() method allowing
            // to specify where consumer should start
            // alreadyConsumedOffset var is quite dummy sync mechanism, only to
            // illustrate the concept
            localConsumer.close();
            localConsumer =
                    new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
            localConsumer.assign(Collections.singletonList(partition0));
            localConsumer.seek(partition0, alreadyConsumedOffset);
            records = localConsumer.poll(3_000);
            assertThat(records.count()).isEqualTo(0);
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_fail_on_seeking_because_subscription_is_lazy() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        // It's almost the same test as the previous one. The difference is that it doesn't use
        // assign() but subscribe() in the reinitialized consumer. By seeking() to particular offset
        // just after, the test is expected to fail because partition is assigned only on poll()
        // for the case of subscribe()
        String testName = "test8_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        try {
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            // It should fail right here because subscribe() is lazy evaluated only
            // on poll() invocation. Here, poll() is made after seek() because
            // we want to control offset by ourselves (not a good idea, just for
            // illustration purpose)
            TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
            localConsumer.seek(partition0, 0L);
            localConsumer.poll(3_000);
            fail("Should fail on seeking to partition when this one is not assigned");
        } catch (IllegalStateException ise) {
            assertThat(ise.getMessage()).isEqualTo("No current assignment for partition singleconsumertopic-0");
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_wake_up_consumer_polling_too_long_without_getting_messages() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test9_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, true, null));

        Collection<Integer> synchronizer = new ArrayList<>();
        new Thread(() -> {
            System.out.println("Waiting 5 seconds before stopping consumer and quit");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            synchronizer.add(1);
            localConsumer.wakeup();
        }).start();
        localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        boolean awaken = false;
        try {
            while (synchronizer.isEmpty()) {
                localConsumer.poll(250);
            }
        } catch (WakeupException we) {
            // WakeupException is expected behaviour when working with wakeup() method
            // So we can do nothing
            awaken = true;
        }

        assertThat(awaken).isTrue();
    }

    @Test
    public void should_fail_on_fetching_too_big_message() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test10_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        // only 1-byte message can be fetched
        consumerProps.setProperty("max.partition.fetch.bytes", "1");
        // use earliest to be sure to consume message sent by producer
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));

        KafkaProducer<String, String> sampleProducer = getProducer(testName);
        sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, "12", "34"));
        sampleProducer.flush();

        // Since we set max fetch size to 1 byte, getting previously flushed message
        // should fail
        localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        try {
            localConsumer.poll(15_000);
            fail("Should fail on getting to big message");
        } catch (RecordTooLargeException rtle) {
            // Got message explains well the problem. It should look like:
            // RecordTooLargeException: There are some messages at [Partition=Offset]:
            // {singleconsumertopic-0=0} whose size is larger than the fetch size 1 and hence cannot be ever returned.
            // Increase the fetch size, or decrease the maximum message size the broker will allow.
        }
    }

    @Test
    public void should_correctly_read_produced_messages_after_consuming_them_by_one_consumer() throws IOException, InterruptedException, ExecutionException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test11_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        // Create now the 2nd consumer, belonging to different group than the 1st one
        // It should consume exactly the same records as the previous consumer
        consumerProps.setProperty("client.id", consumerProps.get("client.id")+"_bis");
        consumerProps.setProperty("group.id", consumerProps.get("group.id") + "_bis");
        KafkaConsumer<String, String> otherConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        try {
            CountDownLatch countDownLatch = new CountDownLatch(2);
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME),
                    new PartitionsStoringRebalanceListener(countDownLatch));
            otherConsumer.subscribe(Collections.singletonList(TOPIC_NAME),
                    new PartitionsStoringRebalanceListener(countDownLatch));
            localConsumer.poll(2_200);
            otherConsumer.poll(2_200);
            countDownLatch.await(5, TimeUnit.SECONDS);

            // Produce some sample messages
            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            String key1 = "1"+ System.currentTimeMillis(), key2 = "2"+System.currentTimeMillis();
            String value1 = "A"+System.currentTimeMillis(), value2 = "B"+ System.currentTimeMillis();
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            sampleProducer.flush();

            // poll for the first time
            ConsumerRecords<String, String> records = localConsumer.poll(3_000);
            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").containsOnly(value1, value2);

            // Check if polled() records are the same
            records = otherConsumer.poll(3_000);
            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").containsOnly(value1, value2);
        } finally {
            localConsumer.close();
            otherConsumer.close();
        }
    }

    @Test
    public void should_reset_to_original_offset_when_demanded_offset_is_out_of_scope() throws IOException, InterruptedException, ExecutionException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test12_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, true, null));
        try {
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            localConsumer.poll(2_500);
            // First, we check which offset was defined before bad seek
            long initialOffset = localConsumer.position(new TopicPartition(TOPIC_NAME, 0));

            // Now, we try to define an out of range seek
            // It shouldn't be take into account
            // According to logs, when expected offset doesn't match the real situation,
            // it's automatically reset :
            // <code>
            // Added fetch request for partition singleconsumertopic-0 at offset 0 (org.apache.kafka.clients.consumer.internals.Fetcher:519)
            // Seeking to offset 10000 for partition singleconsumertopic-0 (org.apache.kafka.clients.consumer.KafkaConsumer:1043)
            // Discarding fetch response for partition singleconsumertopic-0 since its offset 0 does not match the expected offset 10000 (org.apache.kafka.clients.consumer.internals.Fetcher:554)
            // Added fetch request for partition singleconsumertopic-0 at offset 10000 (org.apache.kafka.clients.consumer.internals.Fetcher:519)
            // Fetch offset 10000 is out of range, resetting offset (org.apache.kafka.clients.consumer.internals.Fetcher:595)
            // Resetting offset for partition singleconsumertopic-0 to latest offset. (org.apache.kafka.clients.consumer.internals.Fetcher:290)
            // Fetched offset 0 for partition singleconsumertopic-0 (org.apache.kafka.clients.consumer.internals.Fetcher:483)
            // Added fetch request for partition singleconsumertopic-0 at offset 0 (org.apache.kafka.clients.consumer.internals.Fetcher:519)
            // </code>
            localConsumer.seek(new TopicPartition(TOPIC_NAME, 0), 10000);
            localConsumer.poll(2_500);
            long outOfRangeOffset = localConsumer.position(new TopicPartition(TOPIC_NAME, 0));

            assertThat(outOfRangeOffset).isEqualTo(initialOffset);
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_pause_and_resume_polling() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test13_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, true, null));
        try {
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            localConsumer.poll(2_500);

            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, "12", "34"));
            sampleProducer.flush();

            TopicPartition partition = new TopicPartition(TOPIC_NAME, 0);
            localConsumer.pause(Collections.singletonList(partition));
            // This poll() should return nothing
            ConsumerRecords<String, String> records = localConsumer.poll(2_500);
            assertThat(records.count()).isEqualTo(0);

            // We activate polling
            localConsumer.resume(Collections.singletonList(partition));
            records = localConsumer.poll(2_500);
            assertThat(records.count()).isEqualTo(1);
        } finally {
            localConsumer.close();
        }
    }

    @Test
    public void should_not_fail_on_trying_to_assign_the_same_partition_of_two_consumers_of_the_same_group() throws InterruptedException, IOException, ExecutionException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test14_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        consumerProps.setProperty("client.id", consumerProps.get("client.id")+"_bis");
        KafkaConsumer<String, String> otherConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));;
        try {
            localConsumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
            otherConsumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
            // poll to make sure partitions are assigned
            localConsumer.poll(500);
            otherConsumer.poll(500);

            // Produce some sample messages
            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            String key1 = "1"+ System.currentTimeMillis(), key2 = "2"+System.currentTimeMillis();
            String value1 = "A"+System.currentTimeMillis(), value2 = "B"+ System.currentTimeMillis();
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            sampleProducer.flush();

            TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);
            localConsumer.poll(100);
            localConsumer.seekToBeginning(Collections.singletonList(partition0));
            otherConsumer.poll(100);
            otherConsumer.seekToBeginning(Collections.singletonList(partition0));

            ConsumerRecords<String, String> recordsLocalConsumer = localConsumer.poll(15_000);
            ConsumerRecords<String, String> recordsOtherConsumer = otherConsumer.poll(15_000);

            assertThat(recordsLocalConsumer.count()).isEqualTo(2);
            assertThat(recordsLocalConsumer).hasSameSizeAs(recordsOtherConsumer);
            assertThat(recordsLocalConsumer).extracting("key").containsOnly(key1, key2);
            assertThat(recordsOtherConsumer).extracting("key").containsOnly(key1, key2);
            assertThat(recordsLocalConsumer).extracting("value").containsOnly(value1, value2);
            assertThat(recordsOtherConsumer).extracting("value").containsOnly(value1, value2);
            assertThat(localConsumer.assignment()).hasSameSizeAs(otherConsumer.assignment());
        } finally {
            localConsumer.close();
            otherConsumer.close();
        }
    }

    @Test
    public void should_poll_only_2_from_3_flushed_messages() throws IOException, InterruptedException {
        printAdvice();
        Thread.sleep(10_000);
        String testName = "test15_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        consumerProps.setProperty("auto.offset.reset", "earliest");
        // Property about max polled records was introduced in Kafka 0.10.0
        consumerProps.setProperty("max.poll.records", "2");
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        try {
            localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
            localConsumer.poll(2_200);

            // Produce some sample messages
            KafkaProducer<String, String> sampleProducer = getProducer(testName);
            String key1 = "1", key2 = "2", key3 = "3";
            String value1 = "A", value2 = "B", value3 = "C";
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key1, value1));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key2, value2));
            sampleProducer.send(new ProducerRecord<>(TOPIC_NAME, key3, value3));
            sampleProducer.flush();

            // poll for the first time
            ConsumerRecords<String, String> records = localConsumer.poll(3_000);
            assertThat(records.count()).isEqualTo(2);
            assertThat(records).extracting("key").containsOnly(key1, key2);
            assertThat(records).extracting("value").containsOnly(value1, value2);
        } finally {
            localConsumer.close();
        }
    }


    private KafkaProducer<String, String> getProducer(String testName) throws IOException {
        Properties producerProps = Context.getInstance().getCommonProperties();
        producerProps.setProperty("client.id", ProducerHelper.generateName(TOPIC_NAME, testName));
        return new KafkaProducer<>(ProducerHelper.decorateWithDefaults(producerProps));
    }

    private void printAdvice() {
        System.out.println("Before running this test, please recreate, tested topic:");
        System.out.println("bin/kafka-topics.sh --delete --zookeeper localhost:2181  --topic singleconsumertopic");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic singleconsumertopic");
    }

}

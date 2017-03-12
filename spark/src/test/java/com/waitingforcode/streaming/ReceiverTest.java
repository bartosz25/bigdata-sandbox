package com.waitingforcode.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static com.waitingforcode.util.CollectionHelper.putIfDefined;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Learning tests for customized receivers.
 */
public class ReceiverTest {

    private static final long BATCH_INTERVAL = 100L;
    private static final long TERMINATION_TIME = 2_000L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Receivers Test").setMaster("local[4]");
    private static final StorageLevel DEFAULT_STORAGE = StorageLevel.MEMORY_ONLY();
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;
    public static Queue<Integer> testQueue;


    @Before
    public void initContext() {
        batchContext = new JavaSparkContext(CONFIGURATION);
        streamingContext = new JavaStreamingContext(batchContext, Durations.milliseconds(BATCH_INTERVAL));
        testQueue = new LinkedList<>();
    }

    @After
    public void stopContext() {
        streamingContext.stop(true);
    }

    @Test
    public void should_show_that_onstart_is_blocking_method() throws IOException, InterruptedException {
        triggerDataCreation(0, 2);

        BlockingReceiver blockingReceiver = new BlockingReceiver(DEFAULT_STORAGE);
        JavaReceiverInputDStream<Integer> unreliableReceiverDStream =
                streamingContext.receiverStream(blockingReceiver);
        unreliableReceiverDStream.foreachRDD(rdd -> {});

        long startTime = System.currentTimeMillis();
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(TERMINATION_TIME);

        long runningTime = (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Streaming context was awaiting during " + runningTime + " seconds");

        // This test doesn't have any assertion. Instead it result can be observed
        // through System's prints, which should be:
        // Streaming context was awaiting during 2 seconds
        // Receiver was stopped after 4 seconds
    }

    @Test
    public void should_create_realiable_receiver() throws IOException, InterruptedException {
        triggerDataCreation(0, 15);

        List<List<Integer>> collectedData = new ArrayList<>();
        JavaReceiverInputDStream<Integer> reliableReceiverDStream =
                streamingContext.receiverStream(new ReliableReceiver(DEFAULT_STORAGE));
        reliableReceiverDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), collectedData));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(TERMINATION_TIME);

        assertThat(collectedData).hasSize(3);
        assertThat(collectedData.get(0)).containsOnly(0, 1, 2, 3, 4);
        assertThat(collectedData.get(1)).containsOnly(5, 6, 7, 8, 9);
        assertThat(collectedData.get(2)).containsOnly(10, 11, 12, 13, 14);
    }

    @Test
    public void should_create_unrealiable_receiver() throws IOException, InterruptedException {
        triggerDataCreation(0, 15);

        List<List<Integer>> collectedData = new ArrayList<>();
        JavaReceiverInputDStream<Integer> unreliableReceiverDStream =
                streamingContext.receiverStream(new UnreliableReceiver(DEFAULT_STORAGE));
        unreliableReceiverDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), collectedData));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(TERMINATION_TIME);

        assertThat(collectedData).hasSize(6);
        assertThat(collectedData.get(0)).containsOnly(0, 1, 2);
        assertThat(collectedData.get(1)).containsOnly(3, 4);
        assertThat(collectedData.get(2)).containsOnly(5, 6, 7);
        assertThat(collectedData.get(3)).containsOnly(8, 9);
        assertThat(collectedData.get(4)).containsOnly(10, 11, 12);
        assertThat(collectedData.get(5)).containsOnly(13, 14);
    }

    private static class BlockingReceiver extends Receiver<Integer> {

        private long startTime;
        private long startExecutionEndTime;

        public BlockingReceiver(StorageLevel storageLevel) {
            super(storageLevel);
            startTime = System.currentTimeMillis();
            startExecutionEndTime = TERMINATION_TIME*2 + startTime;
        }

        @Override
        public void onStart() {
            while (System.currentTimeMillis() < startExecutionEndTime) {
                // Data receiving should be started in new thread.
                // Otherwise it's a blocking operation and it doesn't
                // stop at the same time as streaming context
            }

        }

        @Override
        public void onStop() {
            long runningTime = (System.currentTimeMillis() - startTime)/1000;
            System.out.println("Receiver was stopped after " + runningTime + " seconds");
        }

    }

    private static class ReliableReceiver extends Receiver<Integer> {

        private static final int BLOCKS_SIZE = 5;

        List<Integer> blocks = new ArrayList<>();

        private Thread receiver;

        public ReliableReceiver(StorageLevel storageLevel) {
            super(storageLevel);
        }

        @Override
        public void onStart() {
            receiver = new Thread(() -> {
                while (!isStopped()) {
                    if (blocks.size() == BLOCKS_SIZE) {
                        store(blocks.iterator());
                        blocks.clear();
                    }

                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    Integer queueHeadItem = testQueue.poll();
                    if (queueHeadItem != null) {
                        blocks.add(queueHeadItem);
                    }
                }
            });
            receiver.start();
        }

        @Override
        public void onStop() {
            receiver.interrupt();
        }
    }

    private static class UnreliableReceiver extends Receiver<Integer> {
        private Thread receiver;

        public UnreliableReceiver(StorageLevel storageLevel) {
            super(storageLevel);
        }

        @Override
        public void onStart() {
            receiver = new Thread(() -> {
                while (!isStopped()) {
                    Integer queueHeadItem = testQueue.poll();
                    if (queueHeadItem != null) {
                        store(queueHeadItem);
                    }
                    try {
                        Thread.sleep(80);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            receiver.start();
        }

        @Override
        public void onStop() {
            receiver.interrupt();
        }
    }


    public static  void triggerDataCreation(int start, int maxRDDs) throws IOException {
        for (int i = start; i < maxRDDs; i++) {
            testQueue.add(i);
        }
    }

}

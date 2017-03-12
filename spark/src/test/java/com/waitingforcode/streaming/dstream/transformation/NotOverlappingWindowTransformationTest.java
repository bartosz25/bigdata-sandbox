package com.waitingforcode.streaming.dstream.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.waitingforcode.util.CollectionHelper.putIfDefined;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class NotOverlappingWindowTransformationTest {

    private static final String CHECKPOINT_DIRECTORY = "/tmp/spark/checkpoint";
    private static final long BATCH_INTERVAL =  500L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("NotOverlappingWindowTransformation Test").setMaster("local[4]");
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;

    @Before
    public void initContext() {
        batchContext = new JavaSparkContext(CONFIGURATION);
        streamingContext = new JavaStreamingContext(batchContext, Durations.milliseconds(BATCH_INTERVAL));
    }

    @After
    public void stopContext() {
        streamingContext.stop(true);
    }

    @Test
    public void should_generate_not_overlapping_window() throws InterruptedException, IOException {
        JavaReceiverInputDStream<Integer> receiverInputDStream =
                streamingContext.receiverStream(new AutoDataMakingReceiver(StorageLevel.MEMORY_ONLY(), 1_000L, 9));

        JavaDStream<Integer> windowedDStream = receiverInputDStream.window(Durations.milliseconds(1_000L),
                Durations.milliseconds(2_000L));
        List<List<Integer>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(8_000L);

        assertThat(windows.get(0)).isEqualTo(singletonList(0));
        assertThat(windows.get(1)).isEqualTo(singletonList(2));
        assertThat(windows.get(2)).isEqualTo(singletonList(4));
        assertThat(windows.get(3)).isEqualTo(singletonList(6));
    }

    @Test
    public void should_count_in_not_overlapping_window() throws InterruptedException, IOException {
        streamingContext.checkpoint(CHECKPOINT_DIRECTORY);
        JavaReceiverInputDStream<Integer> receiverInputDStream =
                streamingContext.receiverStream(new AutoDataMakingReceiver(StorageLevel.MEMORY_ONLY(), 1_000L, 9));

        JavaDStream<Long> countByWindowDStream = receiverInputDStream.countByWindow(Durations.milliseconds(1_000L),
                Durations.milliseconds(2_000L));
        List<List<Long>> windowsFromCountByWindow = new ArrayList<>(); ;
        countByWindowDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windowsFromCountByWindow));
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(9_000L);

        // Sometimes it returns a sequence of [1, 3, 5, 7]
        assertThat(windowsFromCountByWindow.get(0)).isEqualTo(singletonList(1L));
        assertThat(windowsFromCountByWindow.get(1)).isEqualTo(singletonList(1L));
        assertThat(windowsFromCountByWindow.get(2)).isEqualTo(singletonList(1L));
        assertThat(windowsFromCountByWindow.get(3)).isEqualTo(singletonList(1L));
    }

    @Test
    public void should_reduce_by_not_overlapping_window() throws IOException, InterruptedException {
        JavaReceiverInputDStream<Integer> receiverInputDStream =
                streamingContext.receiverStream(new AutoDataMakingReceiver(StorageLevel.MEMORY_ONLY(), 1_000L, 9));

        JavaDStream<Integer> windowedDStream = receiverInputDStream.reduceByWindow(
                (value1, value2) -> value1 +  value2,
                Durations.milliseconds(1_000L),
                Durations.milliseconds(2_000L)
        );

        List<List<Integer>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(8_000L);

        // Sometimes it returns a sequence of [1, 3, 5, 7]
        assertThat(windows.get(0)).isEqualTo(singletonList(0));
        assertThat(windows.get(1)).isEqualTo(singletonList(2));
        assertThat(windows.get(2)).isEqualTo(singletonList(4));
        assertThat(windows.get(3)).isEqualTo(singletonList(6));
    }


    private static class AutoDataMakingReceiver extends Receiver<Integer> {

        private long sleepingTime;
        private int maxItems;

        public AutoDataMakingReceiver(StorageLevel storageLevel, long sleepingTime, int maxItems) {
            super(storageLevel);
            this.sleepingTime = sleepingTime;
            this.maxItems = maxItems;
        }

        @Override
        public void onStart() {
            try {
                // Give some time to Spark to construct DStream
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
            for (int i = 0; i < maxItems; i++) {
                store(i);
                try {
                    Thread.sleep(sleepingTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onStop() {
            // Do nothing but you should clean data or close persistent connections here
        }
    }


    // 0 sec: producing  (+0)
    // 1 sec: +1
    // 2 sec: +2 - [1]
    // 3 sec: +3
    // 4 sec: +4 - [3]
    // 5 sec: +5
    // 6 sec: +6 - [5]
    // 7 sec: +7
    // 8 sec: +8 - [7]
}

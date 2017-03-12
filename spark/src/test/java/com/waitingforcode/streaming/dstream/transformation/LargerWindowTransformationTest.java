package com.waitingforcode.streaming.dstream.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static com.waitingforcode.util.CollectionHelper.putIfDefined;
import static com.waitingforcode.util.CollectionHelper.triggerDataCreation;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class LargerWindowTransformationTest {

    private static final String CHECKPOINT_DIRECTORY = "/tmp/spark/checkpoint";
    private static final boolean ONE_AT_TIME = true;
    private static final long BATCH_INTERVAL =  500L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("LargerWindowTransformation Test").setMaster("local[4]");
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;
    private Queue<JavaRDD<Integer>> testQueue;

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
    public void should_generate_larger_window() throws InterruptedException, IOException {
        triggerDataCreation(0, 10, batchContext, testQueue);

        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);
        JavaDStream<Integer> windowedDStream = queueDStream.window(Durations.seconds(4),
                Durations.seconds(1));
        List<List<Integer>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> {
            putIfDefined(rdd.collect(), windows);
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(8_000L);

        assertThat(windows).hasSize(8);
        assertThat(windows).containsExactly(
                // window computed at 1st second, so for the time interval ([-3;1])
                Arrays.asList(0, 1),
                // time interval ([-2;2])
                Arrays.asList(0, 1, 2, 3),
                // time interval  ([-1;3])
                Arrays.asList(0, 1, 2, 3, 4, 5),
                // time interval  ([0;4])
                Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7),
                // time interval ([1;5])
                Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9),
                // time interval  ([2;6])
                Arrays.asList(4, 5, 6, 7, 8, 9),
                // time interval  ([3;7])
                Arrays.asList(6, 7, 8, 9),
                // time interval ([4;8])
                Arrays.asList(8, 9)
        );
    }

    @Test
    public void should_count_entries_in_larger_window() throws IOException, InterruptedException {
        streamingContext.checkpoint(CHECKPOINT_DIRECTORY);
        triggerDataCreation(0, 10, batchContext, testQueue);

        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);
        JavaDStream<Long> windowedDStream = queueDStream.countByWindow(Durations.seconds(4),
                Durations.seconds(1));
        List<List<Long>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        assertThat(windows.get(0)).isEqualTo(singletonList(2L));
        assertThat(windows.get(1)).isEqualTo(singletonList(4L));
        assertThat(windows.get(2)).isEqualTo(singletonList(6L));
        assertThat(windows.get(3)).isEqualTo(singletonList(8L));
        assertThat(windows.get(4)).isEqualTo(singletonList(8L));
        assertThat(windows.get(5)).isEqualTo(singletonList(6L));
        assertThat(windows.get(6)).isEqualTo(singletonList(4L));
        assertThat(windows.get(7)).isEqualTo(singletonList(2L));
        assertThat(windows.get(8)).isEqualTo(singletonList(0L));
    }

    @Test
    public void should_reduce_by_larger_window() throws IOException, InterruptedException {
        triggerDataCreation(0, 10, batchContext, testQueue);

        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);
        JavaDStream<Integer> windowedDStream = queueDStream.reduceByWindow(
                (value1, value2) -> value1 +  value2,
                Durations.seconds(4),
                Durations.seconds(1)
        );
        List<List<Integer>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(9_000L);

        assertThat(windows.get(0)).isEqualTo(singletonList(1));
        assertThat(windows.get(1)).isEqualTo(singletonList(6));
        assertThat(windows.get(2)).isEqualTo(singletonList(15));
        assertThat(windows.get(3)).isEqualTo(singletonList(28));
        assertThat(windows.get(4)).isEqualTo(singletonList(44));
        assertThat(windows.get(5)).isEqualTo(singletonList(39));
        assertThat(windows.get(6)).isEqualTo(singletonList(30));
        assertThat(windows.get(7)).isEqualTo(singletonList(17));
    }
}

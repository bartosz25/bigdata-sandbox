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

public class TumblingWindowTransformationTest {

    private static final String CHECKPOINT_DIRECTORY = "/tmp/spark/checkpoint";
    private static final boolean ONE_AT_TIME = true;
    private static final long BATCH_INTERVAL =  500L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("TumblingWindowTransformation Test").setMaster("local[4]");
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
    public void should_generate_tumbling_windows() throws IOException, InterruptedException {
        triggerDataCreation(0, 6, batchContext, testQueue);

        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaDStream<Integer> windowedDStream = queueDStream.window(Durations.seconds(2),
                Durations.seconds(2));

        List<List<Integer>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(5_000L);

        // It proves that tumbling window is a 'perfect' window where
        // items doesn't overlap and aren't lost.
        assertThat(windows).hasSize(2);
        assertThat(windows).containsOnly(
                Arrays.asList(0, 1, 2, 3),
                Arrays.asList(4, 5)
        );
    }

    @Test
    public void should_count_entries_in_tumbling_window() throws IOException, InterruptedException {
        streamingContext.checkpoint(CHECKPOINT_DIRECTORY);
        triggerDataCreation(0, 6, batchContext, testQueue);

        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaDStream<Long> windowedDStream = queueDStream.countByWindow(Durations.seconds(2),
                Durations.seconds(2));

        List<List<Long>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(8_000L);

        assertThat(windows.get(0)).isEqualTo(singletonList(4L));
        assertThat(windows.get(1)).isEqualTo(singletonList(2L));
        assertThat(windows.get(2)).isEqualTo(singletonList(0L));
    }

    @Test
    public void should_reduce_by_tumbling_window() throws IOException, InterruptedException {
        triggerDataCreation(0, 6, batchContext, testQueue);

        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaDStream<Integer> windowedDStream = queueDStream.reduceByWindow(
                (value1, value2) -> value1 +  value2,
                Durations.seconds(2),
                Durations.seconds(2)
        );

        List<List<Integer>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(8_000L);

        assertThat(windows).hasSize(2);
        assertThat(windows).containsExactly(singletonList(6), singletonList(9));
    }

}

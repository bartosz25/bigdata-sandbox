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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static com.waitingforcode.util.CollectionHelper.putIfDefined;
import static com.waitingforcode.util.CollectionHelper.triggerDataCreation;
import static org.assertj.core.api.Assertions.*;

public class WindowTransformationTest {

    private static final boolean ONE_AT_TIME = true;
    private static final long BATCH_INTERVAL =  500L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("WindowTransformation Test").setMaster("local[4]");
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

    // https://github.com/apache/spark/blob/master/streaming/src/test/scala/org/apache/spark/streaming/WindowOperationsSuite.scala
    @Test
    public void should_fail_on_defining_window_operation_when_window_duration_is_invalid() throws InterruptedException {
        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);
        try {
            queueDStream.window(Durations.milliseconds(1_000L));
            fail("Window shouldn't be created with the duration not being a multiple of slide duration");
        } catch (Exception e) {
            assertThat(e.getMessage()).isEqualTo("The window duration of windowed DStream (1000 ms) " +
                    "must be a multiple of the slide duration of parent DStream (1500 ms)");
        }
    }

    @Test
    public void should_fail_on_defining_window_operation_when_window_duration_is_invalid_batch_interval_multiple() throws InterruptedException {
        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);
        try {
            double multiplicationResult = BATCH_INTERVAL*1.5d;
            queueDStream.window(Durations.milliseconds(((long) multiplicationResult)));
            fail("Window shouldn't be created with the duration not being a whole number multiple of slide duration");
        } catch (Exception e) {
            assertThat(e.getMessage()).isEqualTo("The window duration of windowed DStream (2250 ms) " +
                    "must be a multiple of the slide duration of parent DStream (1500 ms)");
        }
    }


    @Test
    public void should_fail_on_defining_window_operation_when_slide_duration_is_invalid_batch_interval_multiple() throws InterruptedException {
        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);
        try {
            double multiplicationResult = BATCH_INTERVAL*1.5d;
            queueDStream.window(Durations.milliseconds(BATCH_INTERVAL*2), Durations.milliseconds(((long) multiplicationResult)));
            fail("Window shouldn't be created with the slide duration not being a whole number multiple of parent slide duration");
        } catch (Exception e) {
            assertThat(e.getMessage()).isEqualTo("The slide duration of windowed DStream (750 ms) " +
                    "must be a multiple of the slide duration of parent DStream (500 ms)");
        }
    }

    @Test
    public void should_window_simple_operation() throws IOException, InterruptedException {
        triggerDataCreation(0, 4, batchContext, testQueue);

        JavaInputDStream<Integer> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        // Batches are retrieved every 1.5 seconds. Window duration is about 3 seconds, so for 4 records we'll
        // have 5 windows: [0], [0, 1], [1, 2], [2, 3], [3].
        JavaDStream<Integer> windowedDStream = queueDStream.window(Durations.seconds(1));

        List<List<Integer>> windows = new ArrayList<>();
        windowedDStream.foreachRDD(rdd -> putIfDefined(rdd.collect(), windows));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(3_000L);

        assertThat(windows).hasSize(5);
        assertThat(windows.get(0)).hasSize(1);
        assertThat(windows.get(0)).containsOnly(0);
        assertThat(windows.get(1)).hasSize(2);
        assertThat(windows.get(1)).containsOnly(0, 1);
        assertThat(windows.get(2)).hasSize(2);
        assertThat(windows.get(2)).containsOnly(1, 2);
        assertThat(windows.get(3)).hasSize(2);
        assertThat(windows.get(3)).containsOnly(2, 3);
        assertThat(windows.get(4)).hasSize(1);
        assertThat(windows.get(4)).containsOnly(3);
    }

}

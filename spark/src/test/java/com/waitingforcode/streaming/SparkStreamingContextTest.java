package com.waitingforcode.streaming;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import java.io.IOException;

import static org.apache.spark.streaming.StreamingContextState.*;
import static org.assertj.core.api.Assertions.*;

public class SparkStreamingContextTest {

    private static final String CHECKPOINT_DIR = "/tmp/spark/checkpoints";

    @Test
    public void should_correctly_start_streaming_context_from_spark_context() {
        // Remember - when running on local mode, always specify the number of
        // threads greater than the number of receivers
        // It's because 1 receiver has always 1 thread reserved to it.
        SparkConf localConf = new SparkConf().setAppName("Streaming Context Test").setMaster("local[1]");
        JavaSparkContext localContext = new JavaSparkContext(localConf);

        // With so that created context our streaming will be divided into batches of
        // 1 second.
        Duration batchInterval = Durations.seconds(1);
        JavaStreamingContext streamingContext = new JavaStreamingContext(localContext, batchInterval);

        assertThat(streamingContext).isNotNull();
    }

    @Test
    public void should_fail_on_starting_context_without_registered_operations() {
        JavaStreamingContext streamingContext = getDefaultContext();
        try {
            streamingContext.start();
            fail("Should not start context without registered operations");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).isEqualTo("requirement failed: No output operations registered, so nothing to execute");
        }
    }

    @Test
    public void should_correctly_trigger_context_states() {
        JavaStreamingContext streamingContext = getDefaultContext();

        StreamingContextState currentContextState = streamingContext.getState();
        assertThat(currentContextState).isEqualTo(INITIALIZED);

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("/tmp/spark/log.out");
        JavaDStream<String> errorLogsDStream = temporarySparkFilesDStream.filter(log -> log.contains("ERROR"));
        // Even if some transformations are registered, there are still
        // no output operations. So we must define one to be able to start the context
        errorLogsDStream.dstream().saveAsTextFiles("errors_", "error");

        streamingContext.start();
        currentContextState = streamingContext.getState();
        assertThat(currentContextState).isEqualTo(ACTIVE);

        streamingContext.stop();
        currentContextState = streamingContext.getState();
        assertThat(currentContextState).isEqualTo(STOPPED);
    }

    @Test
    public void should_fail_on_registering_new_transformation_on_already_active_context() {
        JavaStreamingContext streamingContext = getDefaultContext();

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("/tmp/spark/log.out");
        JavaDStream<String> errorLogsDStream = temporarySparkFilesDStream.filter(log -> log.contains("ERROR"));
        errorLogsDStream.dstream().saveAsTextFiles("errors_", "error");

        streamingContext.start();
        StreamingContextState currentContextState = streamingContext.getState();
        assertThat(currentContextState).isEqualTo(ACTIVE);

        try {
            // Once the context was activated, it's no more possible to add
            // supplementary transformations or output operations.
            // The same rule applies when the context was stopped.
            errorLogsDStream.count();
            fail("Should fail on adding new transformation after staring the context");
        } catch (IllegalStateException ise) {
            assertThat(ise.getMessage()).isEqualTo("Adding new inputs, transformations, and output operations " +
                    "after starting a context is not supported");
        }
    }

    @Test
    public void should_correctly_await_for_termination_during_3_seconds() throws InterruptedException {
        JavaStreamingContext streamingContext = getDefaultContext();

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("/tmp/spark/log.out");
        JavaDStream<String> errorLogsDStream = temporarySparkFilesDStream.filter(log -> log.contains("ERROR"));
        errorLogsDStream.dstream().saveAsTextFiles("errors_", "error");
        streamingContext.start();

        long startTime = System.currentTimeMillis();
        // Blocks the main thread. Thanks to that it can receive new batches
        // infinitely or during specified time (3 seconds in the case)
        streamingContext.awaitTerminationOrTimeout(3_000L);

        assertThat((System.currentTimeMillis() - startTime)/1000).isEqualTo(3L);
        assertThat(streamingContext.getState()).isEqualTo(ACTIVE);
    }

    @Test
    public void should_fail_on_creating_2_streaming_contexts() {
        // As in the case of batch-oriented Spark, only 1 context is allowed
        // per JVM
        JavaStreamingContext streamingContext = getDefaultContext();
        try {
            JavaStreamingContext unexpectedStreamingContext = getDefaultContext();
            fail("Should fail because of creation of the 2nd context which is not allowed");
        } catch (Exception e) {
            assertThat(e.getMessage()).startsWith("Only one SparkContext may be running in this JVM");
        }
    }

    @Test
    public void should_terminate_computation_with_an_exception() throws InterruptedException {
        JavaStreamingContext streamingContext = getDefaultContext();

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("/tmp/spark/log.out");
        JavaDStream<String> errorLogsDStream = temporarySparkFilesDStream.filter(log -> log.contains("ERROR"));
        errorLogsDStream.dstream().saveAsTextFiles("errors_", "error");

        streamingContext.start();

        // Methods to stop computation are: throw an exception and call stop() method.
        // Here we use the stop() method.
        long sleepTime = 3_000L;
        new Thread(() -> {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
            streamingContext.stop();
        }).start();

        long startTime = System.currentTimeMillis();
        streamingContext.awaitTermination();

        assertThat((System.currentTimeMillis() - startTime)/1000).isEqualTo(sleepTime/1000);
    }

    @Test
    public void should_recreate_context_from_checkpoint_file() throws IOException, InterruptedException {
        JavaStreamingContext streamingContext = getDefaultContext();
        streamingContext.checkpoint(CHECKPOINT_DIR);

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("/tmp/spark/log.out");
        JavaDStream<String> errorLogsDStream = temporarySparkFilesDStream.filter(log -> log.contains("ERROR"));
        errorLogsDStream.dstream().saveAsTextFiles("errors_", "error");
        streamingContext.start();

        streamingContext.awaitTerminationOrTimeout(3_000L);
        streamingContext.stop();

        // To restore context from factory method getOrCreate(...) should be used.
        // The first parameter corresponds to directory containing checkpoint context
        // while the second one is a factory method creating a context from scratch.
        streamingContext = JavaStreamingContext.getOrCreate(CHECKPOINT_DIR, () -> getDefaultContext());
        streamingContext.start();
    }

    @Test
    public void should_stop_streaming_and_batch_context_on_stopping_streaming_context() {
        JavaStreamingContext streamingContext = getDefaultContext();

        boolean sparkContextStopped = true;
        streamingContext.stop(sparkContextStopped);

        assertThat(streamingContext.getState()).isEqualTo(STOPPED);
        assertThat(streamingContext.sparkContext().sc().isStopped()).isTrue();
    }

    private static JavaStreamingContext getDefaultContext() {
        SparkConf localConf = new SparkConf().setAppName("Streaming Context Test").setMaster("local[1]");
        JavaSparkContext localContext = new JavaSparkContext(localConf);
        localContext.setCheckpointDir(CHECKPOINT_DIR);

        Duration batchInterval = new Duration(1000);
        return new JavaStreamingContext(localContext, batchInterval);
    }

}

package com.waitingforcode.streaming;

import com.waitingforcode.streaming.dstream.DStreamTest;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CheckpointDStreamInitializationTest {

    private static final boolean STOP_SPARK_CONTEXT = true;
    private static final long BATCH_INTERVAL = 250L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("CheckpointDStreamInitialization Test").setMaster("local[*]");
    private static final String DATA_CHECKPOINT_DIR = "/tmp/streaming_data_checkpoint-accumulator/";

    @Before
    public void clearCheckpointDirectory() throws IOException {
        File checkpointDir = new File(DATA_CHECKPOINT_DIR);
        if (checkpointDir.exists()) {
            FileUtils.cleanDirectory(checkpointDir);
        }
    }

    @Test
    public void should_fail_on_restoring_context_from_checkpoint_because_of_dstream_declaration_out_of_context_scope()
            throws InterruptedException {
        execute(true);
        execute(false);
    }

    private void execute(boolean shouldPass) {
        JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(DATA_CHECKPOINT_DIR, () -> createStreamingContext());
        try {
            JavaDStream<String> temporarySparkFilesDStream =
                    streamingContext.textFileStream("./resources/files/");
            // This method fails when the context is read from checkpoint and
            // DStream is created apart
            temporarySparkFilesDStream.foreachRDD(rdd -> {});
            streamingContext.start();
            streamingContext.awaitTerminationOrTimeout(BATCH_INTERVAL * 2);
            if (!shouldPass) {
                fail("Test should fail because of DStream declared after reading context from the checkpoint");
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            assertThat(throwable.getMessage()).contains("MappedDStream", "has not been initialized");
        } finally {
            streamingContext.stop(STOP_SPARK_CONTEXT);
        }
    }

    private static JavaStreamingContext createStreamingContext() {
        JavaSparkContext batchContext = new JavaSparkContext(CONFIGURATION);
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(batchContext, Durations.milliseconds(BATCH_INTERVAL));
        streamingContext.checkpoint(DATA_CHECKPOINT_DIR);
        return streamingContext;
    }

    @Test
    public void should_correctly_create_context_when_dstream_is_set_inside_context_creation_method()
            throws InterruptedException {
        executeWithCorrectDStreamDefinition();
        Thread.sleep(1_000L);
        executeWithCorrectDStreamDefinition();
    }

    private void executeWithCorrectDStreamDefinition() throws InterruptedException {
        JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(DATA_CHECKPOINT_DIR, () -> createStreamingContextWithProcessing());
        try {
            streamingContext.start();
            streamingContext.awaitTerminationOrTimeout(BATCH_INTERVAL * 2);
        } finally {
            streamingContext.stop(STOP_SPARK_CONTEXT);
        }
    }

    private static JavaStreamingContext createStreamingContextWithProcessing() {
        JavaSparkContext batchContext = new JavaSparkContext(CONFIGURATION);
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(batchContext, Durations.milliseconds(BATCH_INTERVAL));
        streamingContext.checkpoint(DATA_CHECKPOINT_DIR);
        // Unlike other creation method, this one initializes DStream processing
        JavaReceiverInputDStream<String> receiverInputDStream =
                streamingContext.receiverStream(new DStreamTest.AutoDataMakingReceiver(StorageLevel.MEMORY_ONLY(), 500L, 2));
        receiverInputDStream.checkpoint(new     Duration(500L));
        receiverInputDStream.foreachRDD(rdd -> {
            System.out.println("Reading "+rdd.collect());
        });
        return streamingContext;
    }

}

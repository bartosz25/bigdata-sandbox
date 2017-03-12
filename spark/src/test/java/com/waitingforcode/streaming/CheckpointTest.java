package com.waitingforcode.streaming;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Learning tests for data and metadata checkpoints
 * in Spark Streaming.
 */
public class CheckpointTest {

    private static final long BATCH_INTERVAL = 250L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Checkpoint Test").setMaster("local[4]")
                    .set("spark.streaming.receiver.writeAheadLog.enable", "true");
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;
    private static final String DATA_CHECKPOINT_DIR = "/tmp/streaming_data_checkpoint/";


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
    public void should_create_stream_with_data_checkpoint_enabled() throws IOException, InterruptedException {
        streamingContext.checkpoint(DATA_CHECKPOINT_DIR);

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("./resources/files/numbers.txt");
        temporarySparkFilesDStream.foreachRDD(rdd -> {});

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(BATCH_INTERVAL*5);
        streamingContext.stop();

        // To restore context from factory method getOrCreate(...) should be used.
        // The first parameter corresponds to directory containing checkpoint context
        // while the second one is a factory method creating a context from scratch.
        streamingContext = JavaStreamingContext.getOrCreate(DATA_CHECKPOINT_DIR, () -> {
            throw new IllegalStateException("Context should be created from checkpoint");
        });
        streamingContext.start();
    }

    @Test
    public void should_fail_on_trying_to_restore_context_from_not_existent_checkpoint() throws IOException, InterruptedException {
        streamingContext.checkpoint(DATA_CHECKPOINT_DIR);

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("./resources/files/numbers.txt");
        temporarySparkFilesDStream.foreachRDD(rdd -> {});

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(BATCH_INTERVAL*5);
        streamingContext.stop();

        try {
            streamingContext = JavaStreamingContext.getOrCreate("/tmp/not_a_spark_checkpoint_directory", () -> {
                throw new IllegalStateException("Context should be created from checkpoint");
            });
            fail("Should not be able to create context from not checkpointed files");
        } catch (IllegalStateException ise) {
            assertThat(ise.getMessage()).isEqualTo("Context should be created from checkpoint");
        }
    }

    @Test
    public void should_save_data_into_ahead_logs() throws IOException, InterruptedException {
        String aheadLogsDirectory = DATA_CHECKPOINT_DIR+"/receivedBlockMetadata";
        FileUtils.cleanDirectory(new File(aheadLogsDirectory));
        streamingContext.checkpoint(DATA_CHECKPOINT_DIR);

        JavaDStream<String> temporarySparkFilesDStream = streamingContext.textFileStream("./resources/files/numbers.txt");
        temporarySparkFilesDStream.foreachRDD(rdd -> {});

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(BATCH_INTERVAL*5);

        File aheadLogsDir = new File(aheadLogsDirectory);
        List<File> aheadLogs = Lists.newArrayList(aheadLogsDir.listFiles());

        assertThat(aheadLogs).hasSize(1);
        aheadLogs.get(0).getName().startsWith("log-");
    }

}

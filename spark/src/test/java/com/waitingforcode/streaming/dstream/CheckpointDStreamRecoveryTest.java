package com.waitingforcode.streaming.dstream;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckpointDStreamRecoveryTest {

    private static final boolean STOP_SPARK_CONTEXT = true;
    private static final long BATCH_INTERVAL = 2250L;
    private static final String DATA_CHECKPOINT_DIR = "/tmp/streaming_data_checkpoint-recovery/";
    private static final String FAILING_FILE_NAME = "/tmp/streaming-failing-file.txt";
    private static final String REPROCESSING_FILE = "/tmp/streaming-reprocessing-file.txt";

    @Before
    public void clearCheckpointDirectory() throws IOException {
        File checkpointDir = new File(DATA_CHECKPOINT_DIR);
        if (checkpointDir.exists()) {
            FileUtils.cleanDirectory(checkpointDir);
        }
        new File(FAILING_FILE_NAME).delete();
        new File(REPROCESSING_FILE).delete();
    }

    @Test
    public void should_lose_not_processed_data_without_wal_but_with_metadata_checkpoint()
            throws InterruptedException, IOException {
        executeWithCorrectDStreamDefinition(false);
        Thread.sleep(1_000L);
        executeWithCorrectDStreamDefinition(false);
        // Check the content of both files - reprocessing should start with last entries of failed processing
        List<String> reprocessedData = Arrays.asList(
                FileUtils.readFileToString(new File(REPROCESSING_FILE)).split(","));
        List<String> failedData = Arrays.asList(
                FileUtils.readFileToString(new File(FAILING_FILE_NAME)).split(","));
        System.out.println("Comparing "+reprocessedData+ " with failed data " + failedData);
        for (String failedEntry : failedData) {
            assertThat(reprocessedData).doesNotContain(failedEntry);
        }
    }

    // TODO : ale uwaga na batch interval, ktory wpylywa na czyszcznie starych plikow !!!!!!!!
    // Jak sprobujesz wykonac ten kod z 250 ms batch interval, to okaze sie, ze dane, ktore zostaly
    // zapisane, tak naprawde sa czyszczone - mozna to znalezc po odpowiednich plikach w logach (WriteAhead....)
    // i clearing.
    @Test
    public void should_start_processing_from_failure_point_with_wal_enabled() throws InterruptedException, IOException {
        executeWithCorrectDStreamDefinition(true);
        Thread.sleep(5_000L);
        executeWithCorrectDStreamDefinition(true);
        // Check the content of both files - reprocessing should start with last entries of failed processing
        List<String> reprocessedData = Arrays.asList(
                FileUtils.readFileToString(new File(REPROCESSING_FILE)).split(","));
        List<String> failedData = Arrays.asList(
                FileUtils.readFileToString(new File(FAILING_FILE_NAME)).split(","));
        System.out.println("Comparing "+reprocessedData+ " with failed data " + failedData);
        for (String failedEntry : failedData) {
            assertThat(reprocessedData).contains(failedEntry);
        }
    }

    private void executeWithCorrectDStreamDefinition(boolean withWal) throws InterruptedException {
        JavaStreamingContext streamingContext =
                JavaStreamingContext.getOrCreate(DATA_CHECKPOINT_DIR,
                        () -> createStreamingContextWithProcessing(withWal));
        try {
            System.out.println("Starting new processing");
            streamingContext.start();
            streamingContext.awaitTerminationOrTimeout(BATCH_INTERVAL * 15);
        } catch (Exception e) {
            //e.printStackTrace();
        } finally {
            streamingContext.stop(STOP_SPARK_CONTEXT);
        }
    }

    private static JavaStreamingContext createStreamingContextWithProcessing(boolean withWal) {
        System.out.println("Creating new context");
        JavaSparkContext batchContext = new JavaSparkContext(generateConfiguration(withWal));
        JavaStreamingContext streamingContext =
                new JavaStreamingContext(batchContext, Durations.milliseconds(BATCH_INTERVAL));
        streamingContext.checkpoint(DATA_CHECKPOINT_DIR);
        JavaReceiverInputDStream<String> receiverInputDStream =
                streamingContext.receiverStream(new AutoDataMakingReceiver(StorageLevel.MEMORY_ONLY()));
        receiverInputDStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                List<String> consumed = new ArrayList<>();
                // Evaluate content lazily - if an action such as .collect() is called,
                // the processing, even if failed during work, is considered as successful and missed
                // data is not reprocessed after checkpoint restore
                rdd.foreach(text -> {
                    consumed.add(text);
                    //System.out.println("Consuming " + text);
                    if (consumed.size() == 5) {//&& !new File(FAILING_FILE_NAME).exists()) {
                        handleRddConsuming(Collections.singletonList(text));
                        throw new RuntimeException("Error occurred during processing ");
                    }
                });
            }
        });
        return streamingContext;
    }

    private static void handleRddConsuming(List<String> consumedTexts) throws IOException {
        File fileWithFailures = new File(FAILING_FILE_NAME);
        if (fileWithFailures.exists()) {
            File reprocessingFile = new File(REPROCESSING_FILE);
            FileUtils.writeStringToFile(reprocessingFile, String.join(",", consumedTexts));
        } else {
            FileUtils.writeStringToFile(fileWithFailures, String.join(",", consumedTexts));
        }
    }

    public static class AutoDataMakingReceiver extends Receiver<String> {

        public AutoDataMakingReceiver(StorageLevel storageLevel) {
            super(storageLevel);
        }

        @Override
        public void onStart() {
            new Thread(() -> {
                long started = System.currentTimeMillis();
                List<String> data = new ArrayList<>();
                for (int i = 0; i < 11111; i++) {
                    data.add("> " + started);
                    //store("Execution time " + started);
                }
                store(data.iterator());
            }).start();
        }

        @Override
        public void onStop() {
            // Do nothing but you should clean data or close persistent connections here
            System.out.println("Stopping receiver");
        }
    }

    private static SparkConf generateConfiguration(boolean withWal) {
        SparkConf configuration = new SparkConf().setAppName("CheckpointDStreamRecovery Test").setMaster("local[*]")
                .set("spark.streaming.receiver.writeAheadLog.enable", ""+withWal);
        return configuration;
    }
}

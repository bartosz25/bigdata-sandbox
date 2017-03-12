package com.waitingforcode.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.ReliableCheckpointRDD;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckpointTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Checkpoint Test").setMaster("local[1]");
    private static JavaSparkContext context;

    private static final String CHECKPOINT_DIR = "/tmp/checkpoint";

    @BeforeClass
    public static void init() {
        context = new JavaSparkContext(CONFIGURATION);
        context.setCheckpointDir(CHECKPOINT_DIR);
    }

    @Test
    public void should_correctly_checkpoint_a_RDD() {
        JavaRDD<Integer> basicNumbersRDD =
                context.parallelize(IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList()), 2);
        // Checkpoint before an action
        basicNumbersRDD.checkpoint();

        basicNumbersRDD.count();

        assertThat(basicNumbersRDD.isCheckpointed()).isTrue();
        assertThat(basicNumbersRDD.getCheckpointFile().isPresent()).isTrue();
        assertThat(basicNumbersRDD.getCheckpointFile().get()).contains(CHECKPOINT_DIR).contains("rdd-0");
        File checkpointedDir = new File(basicNumbersRDD.getCheckpointFile().get().replace("file:", ""));
        assertThat(checkpointedDir).isDirectory();
        File[] checkpointedFiles = checkpointedDir.listFiles();
        assertThat(checkpointedFiles).hasSize(4);
        // For each saved file we store appropriated checksum (Cyclic Redundancy Check) file
        // Because we've defined the use of 2 partitions, 2 files are saved - one for each partition
        assertThat(checkpointedFiles).extracting("name").containsOnly("part-00000", ".part-00000.crc",
                "part-00001", ".part-00001.crc");
    }

    @Test
    public void should_fail_on_checkpointing_after_action() {
        JavaRDD<Integer> basicNumbersRDD =
                context.parallelize(IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList()), 2);
        basicNumbersRDD.count();

        basicNumbersRDD.checkpoint();

        assertThat(basicNumbersRDD.isCheckpointed()).isFalse();
    }

    @Test
    public void should_correctly_checkpoint_file_in_composed_rdd_flow() {
        JavaRDD<Integer> basicNumbersRDD =
                context.parallelize(IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList()), 2);
        JavaRDD<String> numberNamesRDD = basicNumbersRDD.filter(number -> number > 50)
                .map(number -> "Big number#" + number);
        JavaRDD<String> longNamesRDD = numberNamesRDD.filter(numberName -> numberName.length() > 12);

        longNamesRDD.checkpoint();

        String initialRDDDependencies = longNamesRDD.toDebugString();
        List<String> initialRDDLabels = longNamesRDD.collect();

        // Reinitialize context to see if checkpointed file
        // is reachable
        context.close();
        context = new JavaSparkContext(CONFIGURATION);
        JavaRDD<String> checkpointedLabelsRDD = context.checkpointFile(longNamesRDD.getCheckpointFile().get());
        String checkpointedRDDependencies = checkpointedLabelsRDD.toDebugString();
        List<String> checkpointedRDDLabels = checkpointedLabelsRDD.collect();
        // Check if nothing changed in checkpointed objects
        assertThat(checkpointedLabelsRDD.rdd()).isInstanceOf(ReliableCheckpointRDD.class);
        assertThat(initialRDDDependencies).isNotEqualTo(checkpointedRDDependencies);
        assertThat(checkpointedRDDependencies).startsWith("(2) ReliableCheckpointRDD[0]"); ;
        assertThat(initialRDDLabels).isEqualTo(checkpointedRDDLabels);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_fail_on_reading_checkpoint_from_not_existent_location() {
        context.checkpointFile(CHECKPOINT_DIR+"/not_existent_file");
    }

    @Test
    public void should_correctly_make_a_local_checkpoint() throws InterruptedException {
        JavaRDD<Integer> basicNumbersRDD =
                context.parallelize(IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList()), 2);

        basicNumbersRDD.rdd().localCheckpoint();

        basicNumbersRDD.count();

        String checkpointRDDDebug = basicNumbersRDD.toDebugString();

        // Debug info should look like:
        // (2) ParallelCollectionRDD[0] at parallelize at CheckpointTest.java:104 [Disk Memory Deserialized 1x Replicated]
        // |       CachedPartitions: 2; MemorySize: 2032.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
        // |  LocalCheckpointRDD[1] at count at CheckpointTest.java:108 [Disk Memory Deserialized 1x Replicated]
        assertThat(checkpointRDDDebug).contains("CachedPartitions: 2", "LocalCheckpointRDD[1]");
        assertThat(basicNumbersRDD.getCheckpointFile().isPresent()).isFalse();
    }


}

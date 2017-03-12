package com.waitingforcode.rdd;

import com.waitingforcode.util.FileHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RDDPersistenceTest {

    private final static SparkConf CONFIGURATION =
            new SparkConf().setAppName("RDD Persistence Test").setMaster("local[1]");
    private final static JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);
    private static final String LOGS_FILE = FileHelper.readFileFromClasspath("files/log_entries.txt").getPath();

    @Test
    public void should_persist_rdd_into_memory() {
        JavaRDD<String> infoLogsRDD = CONTEXT.textFile(LOGS_FILE);
        // Similar call could be done with .cache() method
        // It makes a RDD persistent with the default storage level (MEMORY_ONLY)
        infoLogsRDD.persist(StorageLevel.MEMORY_ONLY());
        assertThat(infoLogsRDD.toDebugString()).contains("Memory Deserialized");

        infoLogsRDD.unpersist(true);
        assertThat(infoLogsRDD.toDebugString()).doesNotContain("Memory Deserialized");
    }

    @Test
    public void should_persist_rdd_into_file() throws InterruptedException {
        JavaRDD<String> warnLogsRDD = CONTEXT.textFile(LOGS_FILE);
        // RDDs are persisted in lazy fashion
        // If we call persist() before applying any operation on RDD,
        // the persistance won't be done
        warnLogsRDD.persist(StorageLevel.DISK_ONLY());
        warnLogsRDD.filter(log -> log.contains("WARN"));
        // RDD is persisted automatically on the directory
        // specified in spark.local.dir property (/tmp is the default value)
        // When an RDD is serialized on disk, its debug info should
        // contain "Disk Serialized"
        // If you take a look at RDD.toDebugString method, you'll
        // see that of there are no persistence defined, the output doesn't contain
        // any info about it:
        // val persistence = if (storageLevel != StorageLevel.NONE) storageLevel.description else ""
        assertThat(warnLogsRDD.toDebugString()).contains("Disk Serialized");
    }

}

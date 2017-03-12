package com.waitingforcode.context;


import com.waitingforcode.util.FileHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class SparkContextTest {

    private static final String LOGS_FILE = FileHelper.readFileFromClasspath("files/log_entries.txt").getPath();

    @Test
    public void should_not_read_RDD_after_stopped_context() {
        SparkConf localConf = new SparkConf().setAppName("RDD Test local").setMaster("local[1]");
        JavaSparkContext localContext = new JavaSparkContext(localConf);

        localContext.textFile(LOGS_FILE);
        localContext.stop();
        try {
            localContext.textFile(LOGS_FILE);
            fail("Should fail on reading text file when context is stopped");
        } catch (IllegalStateException ise) {
            assertThat(ise.getMessage()).contains("Cannot call methods on a stopped SparkContext");
        }
    }
}

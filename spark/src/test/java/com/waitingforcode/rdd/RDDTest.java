package com.waitingforcode.rdd;

import com.google.common.collect.Lists;
import com.waitingforcode.util.FileHelper;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

public class RDDTest {

    private final static SparkConf CONFIGURATION =
            new SparkConf().setAppName("RDD Test").setMaster("local[1]").set("spark.driver.allowMultipleContexts", "true");
    private final static JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);
    private static final String NUMBERS_FILE = FileHelper.readFileFromClasspath("files/numbers.txt").getPath();
    private static final String LOGS_FILE = FileHelper.readFileFromClasspath("files/log_entries.txt").getPath();

    @Test(expected = SparkException.class)
    public void should_fail_only_guessing_type_for_not_sequence_file() {
        JavaRDD<Integer> numbers = CONTEXT.objectFile(NUMBERS_FILE);
        // We expect an error because analyzed file is not a Hadoop's SequenceFile
        numbers.collect();
    }

    @Test
    public void should_convert_string_to_integers() {
        // The same test as the previous but this time we use a mapper to convert
        // Strings to Integers
        JavaRDD<String> numbersStringRDD = CONTEXT.textFile(NUMBERS_FILE);
        // Next we use a transformation function which created new RDD holding Integer objects
        // The call can be chained with the previous initialization but for better
        // readability, we prefer to have 2 distinct declarations
        // We can't only collect raw data because the dataset contains badly formatted
        // value (XYZ which throws NumberFormatException). It's the reasony why additional
        // filtering on not null values is expected
        JavaRDD<Integer> numbersRDD = numbersStringRDD.map(RDDTest::integerFromString)
                .filter(nb -> nb != null);

        List<Integer> numbersList = numbersRDD.collect();

        assertThat(numbersList).hasSize(12);
        assertThat(numbersList).containsExactly(1, 2, 3, 10, 111, 145, 180, 200, 201, 239, 250, 290);
    }

    @Test
    public void should_read_integers_correctly_from_hadoops_sequence_file() {
        fail("Implement me");
    }

    @Test
    public void should_not_fail_on_creating_rdd_from_not_existent_file() {
        // RDD are lazy-evaluated. It means that the file is not loaded until the moment
        // when it's not used (transformation or action applied)
        // So this code is completely valid because we don't make any operation on
        // this not existent RDD file
        CONTEXT.textFile(LOGS_FILE+"_not_existent");
    }

    @Test(expected = InvalidInputException.class)
    public void should_fail_on_applying_actions_for_not_existent_file() {
        JavaRDD<String> notExistentLogsRdd = CONTEXT.textFile(LOGS_FILE+"_not_existent");
        // This time we load not existent file to RDD and make some
        // actions on it
        notExistentLogsRdd.collect();
    }

    @Test
    public void should_create_rdd_from_objects_collection() {
        List<Boolean> votesList = Lists.newArrayList(true, false, true, true, true, false);

        JavaRDD<Boolean> votesRDD = CONTEXT.parallelize(votesList);
        Collection<Boolean> votesFor = votesRDD.filter(vote -> vote).collect();

        assertThat(votesFor).hasSize(4);
        assertThat(votesFor).containsOnly(true);
    }

    @Test
    public void should_save_rdd_after_action_application() throws IOException {
        String outputName = "./info_logs_dir";
        try {
            JavaRDD<String> infoLogsRDD = CONTEXT.textFile(LOGS_FILE)
                    .filter(log -> log.contains("INFO"));

            // The output is a directory containing RDD
            // It's why at the begin we remove whole directory
            infoLogsRDD.saveAsTextFile(outputName);

            // Read the file
            assertThat(new File(outputName).exists()).isTrue();
            List<String> infoLogs = Files.readAllLines(Paths.get(outputName + "/part-00000"));
            assertThat(infoLogs).hasSize(4);
            infoLogs.forEach(log -> assertThat(log).contains("INFO"));
        } finally {
            FileHelper.cleanDir(outputName);
        }
    }

    @Test
    public void should_correctly_create_rdd_with_manually_defined_partitions() {
        List<Boolean> boolsList = Lists.newArrayList(true, false, true, true, true, false);

        // Explicitly, we want to store booleans in 3 partitions
        JavaRDD<Boolean> boolsRdd = CONTEXT.parallelize(boolsList, 3);

        // A partition is identified by an index
        List<Partition> partitions = boolsRdd.partitions();
        assertThat(partitions).hasSize(3);
        // In additional, when toDebugString() is called, the first number "(n)" corresponds
        // to the number of partitions
        System.out.println("> "+boolsRdd.toDebugString());
    }

    @Test
    public void should_correctly_cache_rdd() {
        JavaSparkContext closeableContext = new JavaSparkContext(CONFIGURATION);
        List<String> textList = Lists.newArrayList("Txt1", "Txt2", "2", "3", "Txt3", "xt4", "Txt4", "Txt5", "Txt6");

        JavaRDD<String> textRdd = closeableContext.parallelize(textList);

        assertThat(textRdd.getStorageLevel().useMemory()).isFalse();
        System.out.println("> "+textRdd.toDebugString());

        // Caching is based on StorageLevel kept inside each RDD. At the beginning,
        // this level is equal to NONE. Simply it means that RDD is not persisted to
        // cache. In the other side, when cache() (or persist()) method is called on
        // RDD, StorageLevel changes to the configured one. Here, we use default
        // level which stores data only in memory. In the reason why we made a test on
        // useMemory()
        textRdd.cache();
        assertThat(textRdd.getStorageLevel().useMemory()).isTrue();

        // However, data is stored in cache related strictly to given SparkContext
        // So if SparkContext dies, cache is naturally lost
        // SparkContext holds, as TimeStampedWeakValueHashMap, all persistent RDDs.
        closeableContext.close();
        try {
            textRdd.count();
            fail("Should throw an exception when SparkContext is closed, even if RDD is cahced");
        } catch (IllegalStateException ise) {
            // "SparkContext has been shutdown" shows not only that context is not running
            // but also that all operations are passed from RDD to SparkContext
            assertThat(ise.getMessage()).isEqualTo("SparkContext has been shutdown");
        }
    }

    @Test
    public void should_correctly_create_empty_rdd() {
        // as an oddity, we can also create an empty RDD
        JavaRDD<String> emptyRdd = CONTEXT.emptyRDD();

        assertThat(emptyRdd.collect()).isEmpty();
    }

    private static Integer integerFromString(String integerTxt) {
        try {
            return Integer.valueOf(integerTxt);
        } catch (NumberFormatException nfe) {
            nfe.printStackTrace();
        }
        return null;
    }

}

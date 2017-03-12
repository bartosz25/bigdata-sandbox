package com.waitingforcode.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DAG visualisation. Assertions are quite strict (with indices of
 * generated RDDs), so it's better to run them one by one.
 */
public class DAGTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("DAG Test").setMaster("local[1]");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);

    @Test
    public void should_get_dag_from_debug_string_with_only_narrow_transformations() {
        List<String> animals = Arrays.asList("cat", "dog", "fish", "chicken");

        JavaRDD<Integer> filteredNames = CONTEXT.parallelize(animals)
                .filter(name -> name.length() > 3)
                .map(name -> name.length());

        String computedDAG = filteredNames.toDebugString();

        // all created RDDs are computed from the end (from the result)
        // it's the reason why debug method shows first the last RDD (MapPartitionsRDD[2])
        assertThat(computedDAG).containsSequence("MapPartitionsRDD[2]", "MapPartitionsRDD[1]", "ParallelCollectionRDD[0]");
    }

    @Test
    public void should_get_dat_from_debug_string_with_mixed_narrow_and_wide_transformations() {
        List<String> animals = Arrays.asList("cat", "dog", "fish", "chicken", "cow", "fox", "frog", "");

        JavaPairRDD<Character, Iterable<String>> mapped = CONTEXT.parallelize(animals)
                .filter(name -> !name.isEmpty())
                .groupBy(name -> name.charAt(0));

        String computedDAG = mapped.toDebugString();

        // Since groupBy is wide transformation, it triggers data movement
        // among partitions. This fact can be detected, for example,
        // by checking if there are some ShuffledRDD objects. According to scaladoc, this object is:
        // "The resulting RDD from a shuffle (e.g. repartitioning of data)."
        assertThat(computedDAG).containsSequence("MapPartitionsRDD[4]", "ShuffledRDD[3]",
                "MapPartitionsRDD[2]", "MapPartitionsRDD[1]", "ParallelCollectionRDD[0]");
    }

    public void run_ui() {
        // It was used to test Spark UI stages vizualistaion
        // To execute this code, run Spark in standalone way and change the configuration
        //     private static final SparkConf CONFIGURATION =
        //new SparkConf().setAppName("DAG Test").setMaster("spark://bartosz:7077");

        List<String> animals = Arrays.asList("cat", "dog", "fish", "chicken");

        JavaPairRDD<String, Iterable<Integer>> paired = CONTEXT.parallelize(animals)
                .filter(name ->  name.length() > 3)
                .map(name -> name.length())
                .groupBy(nameLength -> "The name has a length of " + nameLength);
        paired.collect();
    }

}

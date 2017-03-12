package com.waitingforcode.rdd;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

public class PartitionTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Partition Test").setMaster("local[1]");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);

    private static final Function<Integer, String> LABEL_MAKER = (number) -> "Number is " + number;
    private static final List<Tuple2<Integer, String>> TEST_DATA = IntStream.rangeClosed(1, 100).boxed()
            .map(number -> new Tuple2<>(number, LABEL_MAKER.apply(number)))
            .collect(Collectors.toList());

    @Test
    public void should_correctly_partition_numbers_through_range_partitioner() {
        // If you're looking at source code of sortByKey(...) method, you'll see that
        // it uses RangePartitioner to dispatch RDD data on specified number of partitions:
        //   def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
        //      : RDD[(K, V)] = self.withScope
        // {
        //    val part = new RangePartitioner(numPartitions, self, ascending)
        //    new ShuffledRDD[K, V, V](self, part)
        //     .setKeyOrdering(if (ascending) ordering else ordering.reverse)
        // }
        int numberOfPartitions = 5;
        JavaPairRDD<Integer, String> rangePartitionedRDD =
                CONTEXT.parallelizePairs(TEST_DATA).sortByKey(true, numberOfPartitions);

        assertThat(rangePartitionedRDD.partitions()).hasSize(5);
        assertThat(rangePartitionedRDD.partitions()).extracting("index").containsOnly(0, 1, 2, 3, 4);
        // Check how data was dispatched among partitions  - normally we expect to have 20 elements by partition
        int t = 0;
        List<Tuple2<Integer, String>>[] dataByPartition = rangePartitionedRDD.collectPartitions(new int[]{0, 1, 2, 3, 4});
        assertThat(dataByPartition[0]).hasSize(20);
        assertThat(dataByPartition[1]).hasSize(20);
        assertThat(dataByPartition[2]).hasSize(20);
        assertThat(dataByPartition[3]).hasSize(20);
        assertThat(dataByPartition[4]).hasSize(20);
        for (int i = 0; i < rangePartitionedRDD.partitions().size(); i++) {
            List<Tuple2<Integer, String>> partitionData = dataByPartition[i];
            for (Tuple2<Integer, String> partitionDataEntry : partitionData) {
                assertThat(partitionDataEntry).isEqualTo(TEST_DATA.get(t));
                t++;
            }
        }
    }

    @Test
    public void should_partition_data_by_range_and_create_less_partitions_than_expected() {
        List<Tuple2<Integer, String>> numbers = IntStream.rangeClosed(1, 100).boxed()
                .map(number -> new Tuple2<>(number%2, "Number is " + number))
                .collect(Collectors.toList());
        int numberOfPartitions = 5;
        JavaPairRDD<Integer, String> rangePartitionedRDD =
                CONTEXT.parallelizePairs(numbers).sortByKey(true, numberOfPartitions);

        // Even if we expect to have 5 partitions, Spark will generate only 3 partitions
        // one for "0" range, one for "1" and one empty
        assertThat(rangePartitionedRDD.partitions()).hasSize(3);
    }

    @Test
    public void should_partition_data_with_hash_partitioner() {
        int numberOfPartitions = 5;
        JavaPairRDD<Integer, String> defaultPartitionedRDD =
                CONTEXT.parallelizePairs(TEST_DATA).partitionBy(new HashPartitioner(numberOfPartitions));
        Optional<Partitioner> partitioner = defaultPartitionedRDD.partitioner();
        assertThat(partitioner.isPresent()).isTrue();
        assertThat(partitioner.get()).isInstanceOf(HashPartitioner.class);

        // Now check how pairs were partitioned
        Map<Integer, List<Integer>> expectedPartitions = new HashMap<>();
        IntStream.rangeClosed(0, 4).forEach(i -> expectedPartitions.put(i, new ArrayList<>()));
        for (int key = 1; key <= 100; key++) {
            int partitionNr = key % numberOfPartitions;
            expectedPartitions.get(partitionNr).add(key);
        }

        List<Tuple2<Integer, String>>[] dataByPartition = defaultPartitionedRDD.collectPartitions(new int[]{0, 1, 2, 3, 4});

        assertThat(dataByPartition[0]).hasSameSizeAs(expectedPartitions.get(0));
        assertThat(dataByPartition[0].stream().map(tuple -> tuple._1()).collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedPartitions.get(0));
        assertThat(dataByPartition[1]).hasSameSizeAs(expectedPartitions.get(1));
        assertThat(dataByPartition[1].stream().map(tuple -> tuple._1()).collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedPartitions.get(1));
        assertThat(dataByPartition[2]).hasSameSizeAs(expectedPartitions.get(2));
        assertThat(dataByPartition[2].stream().map(tuple -> tuple._1()).collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedPartitions.get(2));
        assertThat(dataByPartition[3]).hasSameSizeAs(expectedPartitions.get(3));
        assertThat(dataByPartition[3].stream().map(tuple -> tuple._1()).collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedPartitions.get(3));
        assertThat(dataByPartition[4]).hasSameSizeAs(expectedPartitions.get(4));
        assertThat(dataByPartition[4].stream().map(tuple -> tuple._1()).collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedPartitions.get(4));
    }

    @Test
    public void should_use_coalesce_and_make_expected_changes_on_partitions() {
        // First, we create RDD with 5 partitions by
        // specifying it explicitly
        int numberOfPartitions = 5;
        JavaPairRDD<Integer, String> rangePartitionedRDD = CONTEXT.parallelizePairs(TEST_DATA, numberOfPartitions);

        assertThat(rangePartitionedRDD.partitions()).hasSize(5);

        // Now we reduce the number of partitions
        int newNumberOfPartitions = 3;
        JavaPairRDD<Integer, String> coalescedRDD = rangePartitionedRDD.coalesce(newNumberOfPartitions);

        assertThat(coalescedRDD.partitions()).hasSize(newNumberOfPartitions);

        // If we want to increase the number of partitions,
        // it won't work
        int increasedNumberOfPartitions = 8;
        JavaPairRDD<Integer, String> coalescedIncreasedRDD = coalescedRDD.coalesce(increasedNumberOfPartitions);

        assertThat(coalescedIncreasedRDD.partitions()).hasSize(newNumberOfPartitions);
        assertThat(coalescedIncreasedRDD.toDebugString()).doesNotContain("ShuffledRDD");

        // But if we coalesce with shuffle step enabled,
        // we'll able to increase the number of partitions
        boolean shuffleEnabled = true;
        coalescedIncreasedRDD = coalescedRDD.coalesce(increasedNumberOfPartitions, shuffleEnabled);

        assertThat(coalescedIncreasedRDD.partitions()).hasSize(increasedNumberOfPartitions);
        assertThat(coalescedIncreasedRDD.toDebugString()).contains("ShuffledRDD");
    }

    @Test
    public void should_correctly_repartition_data() {
        int numberOfPartitions = 5;
        JavaPairRDD<Integer, String> rangePartitionedRDD = CONTEXT.parallelizePairs(TEST_DATA, numberOfPartitions);

        // With repartition(...) we can increase and decrease the number of
        // partitions. It's because under-the-hood this method calls
        // coalesce(n, shuffled=true):
        //def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
        //    coalesce(numPartitions, shuffle = true)
        // }
        // Assertions contains exact indices to show better the idea of coalesce use
        // To see test working, run it in separation from the others
        JavaPairRDD<Integer, String> decreasedPartitionsRDD = rangePartitionedRDD.repartition(2);
        assertThat(decreasedPartitionsRDD.partitions()).hasSize(2);
        assertThat(decreasedPartitionsRDD.toDebugString()).contains("ShuffledRDD[2]");

        JavaPairRDD<Integer, String> increasedPartitionsRDD = decreasedPartitionsRDD.repartition(10);
        assertThat(increasedPartitionsRDD.partitions()).hasSize(10);
        assertThat(increasedPartitionsRDD.toDebugString()).contains("ShuffledRDD[6]", "ShuffledRDD[2]");
    }

    @Test
    public void should_repartition_data_with_custom_partitioner() {
        int numberOfPartitions = 5;
        JavaPairRDD<Integer, String> rangePartitionedRDD =
                CONTEXT.parallelizePairs(TEST_DATA).sortByKey(true, numberOfPartitions);

        JavaPairRDD<Integer, String> dummyPartitionedRDD = rangePartitionedRDD
                .repartitionAndSortWithinPartitions(new LongNumbersDiscriminatingPartitioner());

        assertThat(dummyPartitionedRDD.partitions()).hasSize(2);
        assertThat(dummyPartitionedRDD.partitions()).extracting("index").containsOnly(0, 1);
        List<Tuple2<Integer, String>>[] dataByPartition = dummyPartitionedRDD.collectPartitions(new int[]{0, 1});
        assertThat(dataByPartition[0]).hasSize(9);
        assertThat(dataByPartition[0].stream().map(tuple -> tuple._1()).collect(Collectors.toList()))
                .containsOnly(1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(dataByPartition[1]).hasSize(91);
        List<Integer> bigNumbersList =
                dataByPartition[1].stream().map(tuple -> tuple._1()).collect(Collectors.toList());
        for (int i = 10; i <= 100; i++) {
            assertThat(bigNumbersList).contains(i);
        }
    }

    // Dummy partitioner moving pairs with a key lower than 10 to one partition
    // and with bigger or equal to 10 to the other partition
    private static class LongNumbersDiscriminatingPartitioner extends Partitioner {

        @Override
        public int numPartitions() {
            return 2;
        }

        @Override
        public int getPartition(Object key) {
            Integer keyInt = (Integer) key;
            if (keyInt > 9) {
                return 1;
            }
            return 0;
        }
    }

}

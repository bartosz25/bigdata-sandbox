package com.waitingforcode.rdd;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class PerPartitionOperationTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("PerPartitionOperation Test").setMaster("local[1]");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);

    private static final List<Integer> TEST_VALUES = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());

    @Test
    public void should_map_per_partition_with_only_one_randomizer_created_on_each_partition() {
        List<Integer> replacedNumbers = CONTEXT.parallelize(TEST_VALUES, 10)
                .mapPartitions(numbersIterator -> {
                    List<Integer> inputMultiplied = new ArrayList<>();
                    // We want that one random integer replaces all numbers
                    // within a partition
                    Random randomizer = new Random();
                    int replacingValue = randomizer.nextInt(100000);
                    numbersIterator.forEachRemaining(number -> inputMultiplied.add(replacingValue));
                    return inputMultiplied.iterator();
                }).collect();

        Set<Integer> differentReplacingNumbers = new HashSet<>();
        differentReplacingNumbers.addAll(replacedNumbers);
        assertThat(differentReplacingNumbers).hasSize(10);
    }

    @Test
    public void should_iterate_per_partition_elements() {
        CollectionAccumulator<Tuple2<Integer, Integer>> broadcastAccumulator =
                CONTEXT.sc().collectionAccumulator("broadcast accumulator");

        CONTEXT.parallelize(TEST_VALUES, 10).foreachPartition(numbersIterator -> {
            Random randomizer = new Random();
            int replacingValue = randomizer.nextInt(100000);
            // Accumulator used only to test purposes
            numbersIterator.forEachRemaining(number ->
                    broadcastAccumulator.add(new Tuple2<>(number, replacingValue)));
        });

        int previous = broadcastAccumulator.value().get(0)._2();
        for (int i = 0; i < broadcastAccumulator.value().size(); i++) {
            Tuple2<Integer, Integer> tuple = broadcastAccumulator.value().get(i);
            assertThat(tuple._2()).isEqualTo(previous);
            if (tuple._1()%10 == 0 && i+1 < broadcastAccumulator.value().size()) {
                // Check modulo of 10 because we expect to have
                // 10 partitions of 10 elements each
                previous = broadcastAccumulator.value().get(i+1)._2();
            }
        }
    }

    @Test
    public void should_prevent_partitioning_when_key_doesn_t_change() {
        // Boolean flag in mapPartitionsWithIndex(...) is for optimization purposes. By setting
        // it at true, we inform Spark that provided function preserves keys and that it
        // operates on pair RDD.
        // Thanks to this information it knows if shuffle for subsequent transformations
        // (*ByKey, join...) can be avoided. If this flag is true,
        // Spark knows that we, developers, are sure that all data
        // resides in the same partition. By doing so, Spark won't shuffle them.

        // In this test we define the flag to true. In consequence, we should observe
        // only 1 shuffle, because of partitionBy(...) call.
        // And used partitioner doesn't balance well all keys, so it risks to
        // give incorrect results
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = CONTEXT.parallelizePairs(Arrays.asList(
                new Tuple2<>(11, 101), new Tuple2<>(11, 111),
                new Tuple2<>(31, 131), new Tuple2<>(31, 151)
        )).partitionBy(new DummySwappingPartitioner());

        // To illustrate 'preserve partitioning' flag, we use mapPartitionsToPair
        // since under-the-hood both create MapPartitionsRDD in the same
        // way
        JavaPairRDD<Integer, Integer> tuple2JavaRDD =
                integerIntegerJavaPairRDD.mapPartitionsToPair(new TwoValuesMapFunction(), true);

        // now, call transformation supposing to cause shuffling
        JavaPairRDD<Integer, Integer> valuesByKeyPairRDD = tuple2JavaRDD
                .reduceByKey((v1, v2) -> v1+v2);

        tuple2JavaRDD.collect();

        List<Tuple2<Integer, Integer>>[] dataLists = tuple2JavaRDD.collectPartitions(new int[]{0, 1});
        assertThat(dataLists[0]).hasSize(2);
        assertThat(dataLists[0]).contains(new Tuple2<>(11, 2), new Tuple2<>(31, 1));
        assertThat(dataLists[1]).hasSize(2);
        assertThat(dataLists[1]).contains(new Tuple2<>(11, 2), new Tuple2<>(31, 1));
        // one shuffle is expected in the first RDD because of explicit partitioner use
        assertThat(integerIntegerJavaPairRDD.toDebugString()).contains("ShuffledRDD[1]");

        assertThat(valuesByKeyPairRDD.toDebugString()).containsOnlyOnce("ShuffledRDD");
        dataLists = valuesByKeyPairRDD.collectPartitions(new int[]{0, 1});
        assertThat(dataLists[0]).hasSize(2);
        assertThat(dataLists[0]).containsOnly(new Tuple2<>(11, 2), new Tuple2<>(31, 1));
        assertThat(dataLists[1]).hasSize(2);
        assertThat(dataLists[1]).containsOnly(new Tuple2<>(11, 2), new Tuple2<>(31, 1));
    }

    @Test
    public void should_shuffle_even_if_prevents_partition_is_set_to_true() {
        // This test doesn't prevent against shuffle because the flag
        // is set to false to tell Spark that RDD pair keys will change
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = CONTEXT.parallelizePairs(Arrays.asList(
                new Tuple2<>(11, 101), new Tuple2<>(11, 111),
                new Tuple2<>(31, 131), new Tuple2<>(31, 151)
        )).partitionBy(new DummySwappingPartitioner());

        JavaPairRDD<Integer, Integer> tuple2JavaRDD =
                integerIntegerJavaPairRDD.mapPartitionsToPair(new TwoValuesMapFunction(), false);

        // now, call transformation supposing to cause shuffling
        JavaPairRDD<Integer, Integer> valuesByKeyPairRDD = tuple2JavaRDD
                .reduceByKey((v1, v2) -> v1+v2);

        tuple2JavaRDD.collect();

        List<Tuple2<Integer, Integer>>[] dataLists = tuple2JavaRDD.collectPartitions(new int[]{0, 1});
        assertThat(dataLists[0]).hasSize(2);
        assertThat(dataLists[0]).contains(new Tuple2<>(11, 2), new Tuple2<>(31, 1));
        assertThat(dataLists[1]).hasSize(2);
        assertThat(dataLists[1]).contains(new Tuple2<>(11, 2), new Tuple2<>(31, 1));
        // one shuffle is expected in the first RDD because of explicit partitioner use
        assertThat(integerIntegerJavaPairRDD.toDebugString()).contains("ShuffledRDD[1]");

        assertThat(valuesByKeyPairRDD.toDebugString()).contains("ShuffledRDD[1]", "ShuffledRDD[3]");
        dataLists = valuesByKeyPairRDD.collectPartitions(new int[]{0, 1});
        // Because of shuffling, reduceByKey will work correctly. But shuffling
        // makes that we don't know where are stored our tuples, so
        // we make a greedy check
        List<Tuple2<Integer, Integer>> sums = new ArrayList<>();
        dataLists[0].stream().forEach(tuple -> sums.add(tuple));
        dataLists[1].stream().forEach(tuple -> sums.add(tuple));
        assertThat(sums).hasSize(2);
        assertThat(sums).containsOnly(new Tuple2<>(11, 4), new Tuple2<>(31, 2));
    }


    private static class TwoValuesMapFunction
            implements PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer>, Serializable {

        @Override
        public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> pairNumbersIterator) throws Exception {
            List<Tuple2<Integer, Integer>> outputPaired = new ArrayList<>();
            pairNumbersIterator.forEachRemaining(numberTuple -> {
                int pairValue = 1;
                if (numberTuple._1() < 12) {
                    pairValue = 2;
                }
                outputPaired.add(new Tuple2<>(numberTuple._1(), pairValue));
            });
            return outputPaired.iterator();
        }
    }

    private static class DummySwappingPartitioner extends Partitioner {

        private int keyBigNext = 1;
        private int keySmallNext = 0;

        @Override
        public int numPartitions() {
            return 2;
        }

        @Override
        public int getPartition(Object key) {
            Integer keyInt = (Integer) key;
            if (keyInt > 11) {
                int p = keyBigNext;
                keyBigNext = p == 0 ? 1 : 0;
                return p;
            }
            int p = keySmallNext;
            keySmallNext = p == 0 ? 1 : 0;
            return p;
        }
    }
}

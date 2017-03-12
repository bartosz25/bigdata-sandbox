package com.waitingforcode.streaming.dstream.transformation;

import com.google.common.collect.Lists;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * Tests for transformations related to pair DStream.
 */
public class PairDStreamTransformationTest {

    private static final boolean ONE_AT_TIME = true;
    private static final long BATCH_INTERVAL = 1_500L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("PairDStreamTransformation Test").setMaster("local[4]");
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;
    private Queue<JavaRDD<YellowPagesEntry>> testQueue;

    @Before
    public void initContext() {
        batchContext = new JavaSparkContext(CONFIGURATION);
        streamingContext = new JavaStreamingContext(batchContext, Durations.milliseconds(BATCH_INTERVAL));
        testQueue = new LinkedList<>();
    }

    @After
    public void stopContext() {
        streamingContext.stop(true);
    }

    @Test
    public void should_cogroup_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 2, 10, 11);
        triggerDataCreation(0, 2, 12, 13);

        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, !ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));

        testQueue.clear();
        triggerDataCreation(0, 3, 20, 21, 22);
        JavaPairDStream<String, Integer> pairEntriesDStream2 = streamingContext.queueStream(testQueue, !ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));

        // As you will see, cogroup groups data sharing the same key from
        // both DStreams
        JavaPairDStream<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupedPairDStream =
                pairEntriesDStream.cogroup(pairEntriesDStream2);

        Map<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> receivedData = new HashMap<>();
        cogroupedPairDStream.foreachRDD(rdd -> {
            List<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>> collectedData = rdd.collect();
            if (!collectedData.isEmpty()) {
                collectedData.forEach(entry -> receivedData.put(entry._1(), entry._2()));
            }
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        assertThat(receivedData).hasSize(3);
        assertThat(receivedData.get("Person 0")).isNotNull();
        assertThat(receivedData.get("Person 0")._1()).containsOnly(10, 12);
        assertThat(receivedData.get("Person 0")._2()).containsOnly(20);
        assertThat(receivedData.get("Person 1")).isNotNull();
        assertThat(receivedData.get("Person 1")._1()).containsOnly(11, 13);
        assertThat(receivedData.get("Person 1")._2()).containsOnly(21);
        assertThat(receivedData.get("Person 2")).isNotNull();
        assertThat(receivedData.get("Person 2")._1()).isEmpty();
        assertThat(receivedData.get("Person 2")._2()).containsOnly(22);
    }

    @Test
    public void should_union_pair_dstream() throws InterruptedException, IOException {
        triggerDataCreation(0, 5, 10, 11, 12, 13, 14);

        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));

        testQueue.clear();
        triggerDataCreation(0, 10, 210, 211, 212, 213, 214, 15, 16, 17, 18, 19);
        JavaPairDStream<String, Integer> pairEntriesDStream2 = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));

        JavaPairDStream<String, Integer> unionPairDStream = pairEntriesDStream.union(pairEntriesDStream2);

        List<List<Tuple2<String, Integer>>> dataPerRDD = new ArrayList<>();
        unionPairDStream.foreachRDD(rdd -> dataPerRDD.add(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        assertThat(dataPerRDD).hasSize(10);
        assertThat(dataPerRDD.get(0)).hasSize(2);
        assertThat(dataPerRDD.get(0)).containsOnly(new Tuple2<>("Person 0", 10), new Tuple2<>("Person 0", 210));
        assertThat(dataPerRDD.get(1)).hasSize(2);
        assertThat(dataPerRDD.get(1)).containsOnly(new Tuple2<>("Person 1", 11), new Tuple2<>("Person 1", 211));
        assertThat(dataPerRDD.get(2)).hasSize(2);
        assertThat(dataPerRDD.get(2)).containsOnly(new Tuple2<>("Person 2", 12), new Tuple2<>("Person 2", 212));
        assertThat(dataPerRDD.get(3)).hasSize(2);
        assertThat(dataPerRDD.get(3)).containsOnly(new Tuple2<>("Person 3", 13), new Tuple2<>("Person 3", 213));
        assertThat(dataPerRDD.get(4)).hasSize(2);
        assertThat(dataPerRDD.get(4)).containsOnly(new Tuple2<>("Person 4", 14), new Tuple2<>("Person 4", 214));
        assertThat(dataPerRDD.get(5)).hasSize(1);
        assertThat(dataPerRDD.get(5)).containsOnly(new Tuple2<>("Person 5", 15));
        assertThat(dataPerRDD.get(6)).hasSize(1);
        assertThat(dataPerRDD.get(6)).containsOnly(new Tuple2<>("Person 6", 16));
        assertThat(dataPerRDD.get(7)).hasSize(1);
        assertThat(dataPerRDD.get(7)).containsOnly(new Tuple2<>("Person 7", 17));
        assertThat(dataPerRDD.get(8)).hasSize(1);
        assertThat(dataPerRDD.get(8)).containsOnly(new Tuple2<>("Person 8", 18));
        assertThat(dataPerRDD.get(9)).hasSize(1);
        assertThat(dataPerRDD.get(9)).containsOnly(new Tuple2<>("Person 9", 19));
    }

    @Test
    public void should_map_values_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 5, 10, 11, 12, 13, 14);
        triggerDataCreation(0, 3, 20, 21, 22);

        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, !ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));

        JavaPairDStream<String, String> mappedPairDStream =
                pairEntriesDStream.mapValues(telNumber -> makePersonIntro(telNumber));

        List<Tuple2<String, String>> receivedData = new ArrayList<>();
        mappedPairDStream.foreachRDD(rdd -> {
            List<Tuple2<String, String>> collectedData = rdd.collect();
            if (!collectedData.isEmpty()) {
                receivedData.addAll(collectedData);
            }
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        assertThat(receivedData).hasSize(8);
        assertThat(receivedData.get(0)).isEqualTo(new Tuple2<>("Person 0", makePersonIntro(10)));
        assertThat(receivedData.get(1)).isEqualTo(new Tuple2<>("Person 1", makePersonIntro(11)));
        assertThat(receivedData.get(2)).isEqualTo(new Tuple2<>("Person 2", makePersonIntro(12)));
        assertThat(receivedData.get(3)).isEqualTo(new Tuple2<>("Person 3", makePersonIntro(13)));
        assertThat(receivedData.get(4)).isEqualTo(new Tuple2<>("Person 4", makePersonIntro(14)));
        assertThat(receivedData.get(5)).isEqualTo(new Tuple2<>("Person 0", makePersonIntro(20)));
        assertThat(receivedData.get(6)).isEqualTo(new Tuple2<>("Person 1", makePersonIntro(21)));
        assertThat(receivedData.get(7)).isEqualTo(new Tuple2<>("Person 2", makePersonIntro(22)));
    }

    @Test
    public void should_flat_map_values_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 2, 10, 11);

        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, !ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));

        JavaPairDStream<String, String> flatMappedPairDStream =
                pairEntriesDStream.flatMapValues(telNumber -> singletonList(makePersonIntro(telNumber)));
        JavaPairDStream<String, Iterable<String>> mappedPairDStream =
                pairEntriesDStream.mapValues(telNumber -> singletonList(makePersonIntro(telNumber)));

        List<Tuple2<String, String>> receivedDataFromFlatMap = new ArrayList<>();
        flatMappedPairDStream.foreachRDD(rdd -> {
            List<Tuple2<String, String>> collectedData = rdd.collect();
            if (!collectedData.isEmpty()) {
                receivedDataFromFlatMap.addAll(collectedData);
            }
        });
        List<Tuple2<String, Iterable<String>>> receivedDataFromMap = new ArrayList<>();
        mappedPairDStream.foreachRDD(rdd -> {
            List<Tuple2<String, Iterable<String>>> collectedData = rdd.collect();
            if (!collectedData.isEmpty()) {
                receivedDataFromMap.addAll(collectedData);
            }
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(5_000L);

        // The difference between map and flat map is, once again, the fact of
        // flattening entries in the second. For the same function, map
        // returns a collection of data.
        assertThat(receivedDataFromFlatMap).hasSize(2);
        assertThat(receivedDataFromFlatMap.get(0)).isEqualTo(new Tuple2<>("Person 0", makePersonIntro(10)));
        assertThat(receivedDataFromFlatMap.get(1)).isEqualTo(new Tuple2<>("Person 1", makePersonIntro(11)));
        assertThat(receivedDataFromMap).hasSize(2);
        assertThat(receivedDataFromMap.get(0)).isEqualTo(new Tuple2<>("Person 0", singletonList(makePersonIntro(10))));
        assertThat(receivedDataFromMap.get(1)).isEqualTo(new Tuple2<>("Person 1", singletonList(makePersonIntro(11))));
    }

    @Test
    public void should_combine_by_key_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 3, 10, 11, 12);
        triggerDataCreation(0, 3, 20, 21, 22);
        triggerDataCreation(0, 2, 30, 31);

        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, !ONE_AT_TIME)
                .mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber));

        // DStream's combineByKey(...) is similar to RDD's combineByKey(...). First,
        // we create initialization function. After we create a function working on initialized
        // element with subsequent treated items. Finally we merge combined results.
        // The last parameter is partitioner to use.
        JavaPairDStream<String, List<Integer>> combinedPairDStream = pairEntriesDStream.combineByKey(
                Lists::newArrayList,
                (telNumbersList, telNumber) -> {
                    telNumbersList.add(telNumber);
                    return telNumbersList;
                },
                (telNumbersList1, telNumbersList2) -> {
                    telNumbersList1.addAll(telNumbersList2);
                    return telNumbersList1;
                },
                new SinglePartitionPartitioner()
        );
        Map<String, List<Integer>> receivedData = new HashMap<>();
        combinedPairDStream.foreachRDD(rdd -> {
            rdd.collectAsMap().forEach((person, telNumbers) -> receivedData.put(person, telNumbers));
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        assertThat(receivedData).hasSize(3);
        assertThat(receivedData.get("Person 0")).isNotNull();
        assertThat(receivedData.get("Person 0")).hasSize(3).containsOnly(10, 20, 30);
        assertThat(receivedData.get("Person 1")).isNotNull();
        assertThat(receivedData.get("Person 1")).hasSize(3).containsOnly(11, 21, 31);
        assertThat(receivedData.get("Person 2")).isNotNull();
        assertThat(receivedData.get("Person 2")).hasSize(2).containsOnly(12, 22);
    }

    @Test
    public void should_reduce_by_key_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 5, 10, 11, 12, 13, 14);
        triggerDataCreation(0, 3, 20, 21, 22);

        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, !ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));

        JavaPairDStream<String, Integer> groupedByKeyPairDStream = pairEntriesDStream
                .reduceByKey((v1, v2) -> v1+v2);
        Map<String, Integer> receivedData = new HashMap<>();
        groupedByKeyPairDStream.foreachRDD(rdd -> {
            Map<String, Integer> collectedData = rdd.collectAsMap();
            if (!collectedData.isEmpty()) {
                collectedData.forEach((person, sum) -> receivedData.put(person, sum));
            }
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(5_000L);

        assertThat(receivedData).hasSize(5);
        assertThat(receivedData.get("Person 0")).isNotNull();
        assertThat(receivedData.get("Person 0")).isEqualTo(30);
        assertThat(receivedData.get("Person 1")).isNotNull();
        assertThat(receivedData.get("Person 1")).isEqualTo(32);
        assertThat(receivedData.get("Person 2")).isNotNull();
        assertThat(receivedData.get("Person 2")).isEqualTo(34);
        assertThat(receivedData.get("Person 3")).isNotNull();
        assertThat(receivedData.get("Person 3")).isEqualTo(13);
        assertThat(receivedData.get("Person 4")).isNotNull();
        assertThat(receivedData.get("Person 4")).isEqualTo(14);
    }

    @Test
    public void should_group_by_key_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 5, 10, 11, 12, 13, 14);
        triggerDataCreation(0, 3, 20, 21, 22);

        // If 'one at time' is false, received data will be 1 per RDD,
        // so the grouping won't be done.
        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, !ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        JavaPairDStream<String, Iterable<Integer>> groupedByKeyPairDStream = pairEntriesDStream.groupByKey();
        List<Tuple2<String, Iterable<Integer>>> receivedData = new ArrayList<>();
        groupedByKeyPairDStream.foreachRDD(rdd -> {
            List<Tuple2<String, Iterable<Integer>>> collectedData = rdd.collect();
            if (!collectedData.isEmpty()) {
                receivedData.addAll(collectedData);
            }
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        assertThat(receivedData).hasSize(5);
        Tuple2<String, Iterable<Integer>> person0 = findPerson(receivedData, "Person 0");
        assertThat(person0._2()).containsOnly(10, 20);
        Tuple2<String, Iterable<Integer>> person1 = findPerson(receivedData, "Person 1");
        assertThat(person1._2()).containsOnly(11, 21);
        Tuple2<String, Iterable<Integer>> person2 = findPerson(receivedData, "Person 2");
        assertThat(person2._2()).containsOnly(12, 22);
        Tuple2<String, Iterable<Integer>> person3 = findPerson(receivedData, "Person 3");
        assertThat(person3._2()).containsOnly(13);
        Tuple2<String, Iterable<Integer>> person4 = findPerson(receivedData, "Person 4");
        assertThat(person4._2()).containsOnly(14);
    }

    @Test
    public void should_join_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 5, 10, 11, 12, 13, 14);
        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        testQueue.clear();

        triggerDataCreation(0, 3, 20, 21, 22);
        JavaPairDStream<String, Integer> pairEntriesDStream2 = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        JavaPairDStream<String, Tuple2<Integer, Integer>> joinedPairDStream = pairEntriesDStream.join(pairEntriesDStream2);
        Map<String, Tuple2<Integer, Integer>> receivedData = new HashMap<>();
        joinedPairDStream.foreachRDD(rdd -> {
            rdd.collectAsMap().forEach((person, joinedTelNumbers) -> receivedData.put(person, joinedTelNumbers));
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        // Only 3 values are expected because join is strict - if there are no common
        // values in both DStream's RDDs, they won't be merged
        assertThat(receivedData).hasSize(3);
        assertThat(receivedData.get("Person 0")).isNotNull();
        assertThat(receivedData.get("Person 0")).isEqualTo(new Tuple2<>(10, 20));
        assertThat(receivedData.get("Person 1")).isNotNull();
        assertThat(receivedData.get("Person 1")).isEqualTo(new Tuple2<>(11, 21));
        assertThat(receivedData.get("Person 2")).isNotNull();
        assertThat(receivedData.get("Person 2")).isEqualTo(new Tuple2<>(12, 22));
    }

    @Test
    public void should_left_outer_join_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 3, 10, 11, 12);
        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        testQueue.clear();

        triggerDataCreation(0, 2, 20, 21);
        JavaPairDStream<String, Integer> pairEntriesDStream2 = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        JavaPairDStream<String, Tuple2<Integer, Optional<Integer>>> joinedPairDStream =
                pairEntriesDStream.leftOuterJoin(pairEntriesDStream2);
        Map<String, Tuple2<Integer, Optional<Integer>>> receivedData = new HashMap<>();
        joinedPairDStream.foreachRDD(rdd -> {
            rdd.collectAsMap().forEach((person, joinedTelNumbers) -> receivedData.put(person, joinedTelNumbers));
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        // Left outer join includes all values from the left pair DStream
        // and only the matching ones from the right DStream
        assertThat(receivedData).hasSize(3);
        assertThat(receivedData.get("Person 0")).isNotNull();
        assertThat(receivedData.get("Person 0")).isEqualTo(new Tuple2<>(10, Optional.of(20)));
        assertThat(receivedData.get("Person 1")).isNotNull();
        assertThat(receivedData.get("Person 1")).isEqualTo(new Tuple2<>(11, Optional.of(21)));
        assertThat(receivedData.get("Person 2")).isNotNull();
        assertThat(receivedData.get("Person 2")).isEqualTo(new Tuple2<>(12, Optional.absent()));
    }

    @Test
    public void should_right_outer_join_for_pair_dstream() throws InterruptedException, IOException {
        triggerDataCreation(0, 2, 10, 11);
        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        testQueue.clear();

        triggerDataCreation(0, 3, 20, 21, 22);
        JavaPairDStream<String, Integer> pairEntriesDStream2 = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        JavaPairDStream<String, Tuple2<Optional<Integer>, Integer>> joinedPairDStream =
                pairEntriesDStream.rightOuterJoin(pairEntriesDStream2);
        Map<String, Tuple2<Optional<Integer>, Integer>> receivedData = new HashMap<>();
        joinedPairDStream.foreachRDD(rdd -> {
            rdd.collectAsMap().forEach((person, joinedTelNumbers) -> receivedData.put(person, joinedTelNumbers));
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        // Right outer join works inversely to the left outer join - it includes all values from
        // the right pair DStream and only the matching ones from the right DStream
        assertThat(receivedData).hasSize(3);
        assertThat(receivedData.get("Person 0")).isNotNull();
        assertThat(receivedData.get("Person 0")).isEqualTo(new Tuple2<>(Optional.of(10), 20));
        assertThat(receivedData.get("Person 1")).isNotNull();
        assertThat(receivedData.get("Person 1")).isEqualTo(new Tuple2<>(Optional.of(11), 21));
        assertThat(receivedData.get("Person 2")).isNotNull();
        assertThat(receivedData.get("Person 2")).isEqualTo(new Tuple2<>(Optional.absent(), 22));
    }

    @Test
    public void should_full_outer_join_for_pair_dstream() throws IOException, InterruptedException {
        triggerDataCreation(0, 2, 10, 11);
        JavaPairDStream<String, Integer> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        testQueue.clear();

        triggerDataCreation(2, 4, 0, 0, 20, 21);
        JavaPairDStream<String, Integer> pairEntriesDStream2 = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(entry -> new Tuple2<>(entry.person, entry.telNumber)));
        JavaPairDStream<String, Tuple2<Optional<Integer>, Optional<Integer>>> joinedPairDStream =
                pairEntriesDStream.fullOuterJoin(pairEntriesDStream2);
        Map<String, Tuple2<Optional<Integer>, Optional<Integer>>> receivedData = new HashMap<>();
        joinedPairDStream.foreachRDD(rdd -> {
            rdd.collectAsMap().forEach((person, joinedTelNumbers) -> receivedData.put(person, joinedTelNumbers));
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        // Full outer join takes all data from both DStreams
        assertThat(receivedData).hasSize(4);
        assertThat(receivedData.get("Person 0")).isNotNull();
        assertThat(receivedData.get("Person 0")).isEqualTo(new Tuple2<>(Optional.of(10), Optional.absent()));
        assertThat(receivedData.get("Person 1")).isNotNull();
        assertThat(receivedData.get("Person 1")).isEqualTo(new Tuple2<>(Optional.of(11), Optional.absent()));
        assertThat(receivedData.get("Person 2")).isNotNull();
        assertThat(receivedData.get("Person 2")).isEqualTo(new Tuple2<>(Optional.absent(), Optional.of(20)));
        assertThat(receivedData.get("Person 3")).isNotNull();
        assertThat(receivedData.get("Person 3")).isEqualTo(new Tuple2<>(Optional.absent(), Optional.of(21)));
    }

    private static String makePersonIntro(int telNumber) {
        int modulo = telNumber%10;
        return "This is person "+modulo + " with tel number "+telNumber;
    }

    private static <T> Tuple2<String, T> findPerson(List<Tuple2<String, T>> receivedData, String personLabel) {
        return receivedData.stream().filter(tuple -> tuple._1().equals(personLabel)).findAny().get();
    }

    private void triggerDataCreation(int startIndex, int maxRDDs, int...telNumbers) throws IOException {
        for (int i = startIndex; i < maxRDDs; i++) {
            JavaRDD<YellowPagesEntry> newRDD1 =
                    batchContext.parallelize(Arrays.asList(new YellowPagesEntry("Person " + i, telNumbers[i])));
            testQueue.add(newRDD1);
        }
    }

    private static class YellowPagesEntry implements Serializable {
        private String person;

        private int telNumber;

        public YellowPagesEntry(String person, int telNumber) {
            this.person = person;
            this.telNumber = telNumber;
        }
    }

    private static class SinglePartitionPartitioner extends Partitioner implements Serializable {

        @Override
        public int numPartitions() {
            return 1;
        }

        @Override
        public int getPartition(Object key) {   System.out.println("Partition for key " + key);
            return 0;
        }
    }

}

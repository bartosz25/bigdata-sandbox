package com.waitingforcode.streaming.dstream.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for simple transformations in Spark Streaming. The transformations
 * specific to streaming processing, such as window and state oriented, will be
 * tested in other test classes.
 */
public class TransformationTest {

    private static final boolean ONE_AT_TIME = true;
    private static final long BATCH_INTERVAL = 1_500L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Transformation Test").setMaster("local[4]");
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;
    private Queue<JavaRDD<String>> testQueue;

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
    public void should_filter_dstream() throws InterruptedException, IOException {
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaDStream<String> filteredDStream = queueDStream.filter(label -> label.equals(makeTestLabel(0)));
        List<String> receivedData = new ArrayList<>();
        filteredDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        assertThat(receivedData).hasSize(1);
        assertThat(receivedData).containsOnly(makeTestLabel(0));
    }

    @Test
    public void should_map_dstream() throws InterruptedException, IOException {
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaDStream<Integer> mappedDStream = queueDStream.map(label -> toInt(label));
        List<Integer> receivedData = new ArrayList<>();
        mappedDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        assertThat(receivedData).hasSize(4);
        assertThat(receivedData).containsOnly(0, 1, 2, 3);
    }

    @Test
    public void should_flatmap_dstream() throws IOException, InterruptedException {
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaDStream<Integer> flatMappedDStream = queueDStream.flatMap(label -> Arrays.asList(toInt(label)).iterator());
        JavaDStream<List<Integer>> mappedDStream = queueDStream.map(label -> Arrays.asList(toInt(label)));

        List<Integer> receivedFlatMappedData = new ArrayList<>();
        flatMappedDStream.foreachRDD(rdd -> receivedFlatMappedData.addAll(rdd.collect()));
        List<List<Integer>> receivedMappedData = new ArrayList<>();
        mappedDStream.foreachRDD(listJavaRDD -> receivedMappedData.addAll(listJavaRDD.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        // As you can observe below, the difference between flat map and map consists
        // on format of returned items. For simple map function, when we deal with a collection of
        // elements (in our case, single item lists), DStream's RDDs will contain
        // a lists of lists with data ( [ [0], [1], [2], [3] ] ).
        // In the case of flat map they will have flattened lists with data ( [0, 1, 2, 3] ).
        assertThat(receivedFlatMappedData).hasSize(4);
        assertThat(receivedFlatMappedData).containsOnly(0, 1, 2, 3);
        assertThat(receivedMappedData).hasSize(4);
        assertThat(receivedMappedData).containsOnly(Arrays.asList(0), Arrays.asList(1), Arrays.asList(2), Arrays.asList(3));
    }

    @Test
    public void should_map_partition() throws IOException, InterruptedException {
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);
        // For test purposes we set a simple common prefix but you'd imagine here
        // a slow database or web service connection retrieving a lot
        // of objects shared by all transformed items
        String prefix = ">>>> ";

        JavaDStream<String> partitionMappedDStream = queueDStream.mapPartitions(labelsIterator -> {
            List<String> prefixedLabels = new ArrayList<>();
            // labelsIterator contains all items within treated RDD's partition
            labelsIterator.forEachRemaining(label -> prefixedLabels.add(prefix + label));
            return prefixedLabels.iterator();
        });
        List<String> receivedData = new ArrayList<>();
        partitionMappedDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        assertThat(receivedData).hasSize(4);
        assertThat(receivedData).containsOnly(prefix + makeTestLabel(0), prefix + makeTestLabel(1),
                prefix + makeTestLabel(2), prefix + makeTestLabel(3));
    }

    @Test
    public void should_glom_dstream() throws IOException, InterruptedException {
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        // It coalesces all elements of given partition to a single array.
        // Representing RDD as flattened array helps to avoid shuffling in some situations, as for
        // example the maximum retrieval described in this post:
        // http://blog.madhukaraphatak.com/glom-in-spark/
        JavaDStream<List<String>> glommedDStream = queueDStream.glom();

        List<Integer> partitionSizes = new ArrayList<>();
        List<List<String>> receivedData = new ArrayList<>();
        glommedDStream.foreachRDD(rdd -> {
            partitionSizes.add(rdd.partitions().size());
            System.out.println("Got " + rdd.collect());
            receivedData.addAll(rdd.collect());
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        // We expect to have RDDs of 4 partitions when stream read data and 1 partition when
        // there were no data to read.
        // In consequence glom operates on below RDDs:
        // * 1st data composed by 4 single item partitions  ["Number 0", "", "", ""]
        // * 2nd data composed by 4 single item partitions  ["Number 1", "", "", ""]
        // * 3rd data composed by 4 single item partitions  ["Number 2", "", "", ""]
        // * 4th data composed by 4 single item partitions  ["Number 3", "", "", ""]
        // * the others composed by a single empty partition [""]
        // After calling RDD.collect(), the result of glom is below: [
        //   [], [], [], [Number 0], [], [], [], [Number 1], [], [], [], [Number 2], [], [], [], [Number 3], [], [], []
        // ] <- common array of all coalesced RDDs
        // If given partition had more than 1 element, they would be put together into common array.
        assertThat(receivedData).hasSize(partitionSizes.stream().reduce((v1, v2) -> v1 + v2).get());
        assertThat(receivedData).containsOnly(
                singletonList(makeTestLabel(0)), singletonList(makeTestLabel(1)),
                singletonList(makeTestLabel(2)), singletonList(makeTestLabel(3)),
                emptyList());
        assertThat(partitionSizes.subList(0, 3)).containsOnly(4);
        assertThat(partitionSizes.subList(4, partitionSizes.size())).containsOnly(1);
    }

    @Test
    public void should_repartition_dstream() throws IOException, InterruptedException {
        triggerDataCreation(4);
        boolean notOneAtTime = false;
        // Start by checking the number of initial partitions
        JavaDStream<String> queueDStream = streamingContext.queueStream(testQueue, notOneAtTime);
        Set<Integer> initialPartitionSizes = new HashSet<>();
        queueDStream.foreachRDD(rdd -> {
            if (!rdd.collect().isEmpty()) {
                initialPartitionSizes.add(rdd.partitions().size());
            }
        });
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(5_000L);
        streamingContext.stop();

        // Restart context and check partitions after repartition
        JavaStreamingContext localStreamingContext =
                new JavaStreamingContext(CONFIGURATION, Durations.milliseconds(BATCH_INTERVAL));
        queueDStream = localStreamingContext.queueStream(testQueue, notOneAtTime);
        JavaDStream<String> repartitionedDStream = queueDStream.repartition(2);
        Set<Integer> repartitionedSizes = new HashSet<>();
        repartitionedDStream.foreachRDD(rdd -> {
            repartitionedSizes.add(rdd.partitions().size());
        });
        localStreamingContext.start();
        localStreamingContext.awaitTerminationOrTimeout(5_000L);

        assertThat(repartitionedSizes).containsOnly(2);
        assertThat(initialPartitionSizes).doesNotContain(2);
    }

    @Test
    public void should_reduce_dstream() throws IOException, InterruptedException {
        triggerDataCreation(4);
        // Reduce returns a new DStream. It's composed by RDDs constructed by applying
        // reduce operations on them. So to see reduce() works, we need to ingest more than 1 item
        // at time to our queue stream.
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, !ONE_AT_TIME);

        JavaDStream<String> reducedDStream = queueDStream.reduce(new LabelsConcatenator());

        List<String> receivedData = new ArrayList<>();
        reducedDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        assertThat(receivedData).hasSize(1);
        assertThat(receivedData.get(0)).isEqualTo("Labels are: Number 0, Number 1, Number 2, Number 3");
    }

    @Test
    public void should_count_rdds_in_dstream() throws InterruptedException, IOException {
        triggerDataCreation(5);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaDStream<Long> countDStream = queueDStream.map(label -> toInt(label))
                .filter(number -> number > 2)
                .count();

        List<Long> receivedData = new ArrayList<>();
        countDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(8_000L);

        assertThat(receivedData).hasSize(5);
        // count() is applied per RDD. Since queueStream gets 1 RDD at time,
        // it's normal to have 5 counters
        assertThat(receivedData).containsOnly(0L, 0L, 0L, 1L, 1L);
    }

    @Test
    public void should_transform_dstream() throws IOException, InterruptedException {
        triggerDataCreation(5);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        // transform() provides more fine-grained control on
        // transformations applied on each RDD held by
        // given DStream
        JavaDStream<Integer> transformedDStream = queueDStream.transform(stringRDD -> stringRDD
                .filter(text -> !text.isEmpty())
                .map(label -> toInt(label)));

        List<Integer> receivedData = new ArrayList<>();
        transformedDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(8_000L);

        assertThat(receivedData).hasSize(5);
        assertThat(receivedData).containsOnly(0, 1, 2, 3, 4);
    }

    @Test
    public void should_transform_dstream_to_pair() throws IOException, InterruptedException {
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaPairDStream<String, Integer> pairDStream = queueDStream.transformToPair(labelRDD ->
                labelRDD.mapToPair(label -> new Tuple2<>(label, toInt(label))));

        List<Tuple2<String, Integer>> receivedData = new ArrayList<>();
        pairDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        assertThat(receivedData).hasSize(4);
        assertThat(receivedData).contains(new Tuple2<>(makeTestLabel(0), 0), new Tuple2<>(makeTestLabel(1), 1),
                new Tuple2<>(makeTestLabel(2), 2), new Tuple2<>(makeTestLabel(3), 3));
    }

    @Test
    public void should_join_two_dstreams_with_union_transformation() throws IOException, InterruptedException {
        triggerDataCreation(5);
        boolean oneAtTime = false;
        JavaInputDStream<String> queueDStreamFirst5 = streamingContext.queueStream(testQueue, oneAtTime);
        testQueue.clear();
        triggerDataCreation(5, 15);
        JavaInputDStream<String> queueDStreamFirst10 = streamingContext.queueStream(testQueue, oneAtTime);

        JavaDStream<String> jointDStream = queueDStreamFirst5.union(queueDStreamFirst10);

        List<String> receivedData = new ArrayList<>();
        jointDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(12_000L);

        assertThat(receivedData).hasSize(15);
        assertThat(receivedData).containsOnly(makeTestLabel(0), makeTestLabel(1), makeTestLabel(2), makeTestLabel(3),
                makeTestLabel(4), makeTestLabel(5), makeTestLabel(6), makeTestLabel(7), makeTestLabel(8),
                makeTestLabel(9), makeTestLabel(10), makeTestLabel(11), makeTestLabel(12), makeTestLabel(13),
                makeTestLabel(14));
    }

    @Test
    public void should_get_sliced_data_from_dstream() throws IOException, InterruptedException {
        // Purely speaking, slice is not a transformation but it deserves
        // its place in this examples list because it's widely used
        // by other transformations, as window-based ones
        triggerDataCreation(5);
        long startTime = System.currentTimeMillis();
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        queueDStream.filter(t -> true).foreachRDD(rdd -> {});
        streamingContext.start();


        // The first received RDD should be returned here
        List<JavaRDD<String>> slicedDStream =
                queueDStream.slice(new Time(startTime), new Time(startTime+BATCH_INTERVAL));
        assertThat(slicedDStream).hasSize(1);
        assertThat(slicedDStream.get(0).collect()).containsOnly(makeTestLabel(0));
        // Here 2 first RDDs should be returned
        slicedDStream =
                queueDStream.slice(new Time(startTime), new Time(startTime+BATCH_INTERVAL*2));
        assertThat(slicedDStream).hasSize(2);
        assertThat(slicedDStream.get(0).collect()).containsOnly(makeTestLabel(0));
        assertThat(slicedDStream.get(1).collect()).containsOnly(makeTestLabel(1));

        streamingContext.awaitTerminationOrTimeout(8_000L);
    }

    @Test
    public void should_apply_count_by_value_when_all_rdds_are_in_dstream() throws IOException, InterruptedException {
        boolean notOneAtTime = false;
        triggerDataCreation(4);
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, notOneAtTime);

        JavaPairDStream<String, Long> counterByValueDStream = queueDStream.countByValue();

        List<Tuple2<String, Long>> receivedData = new ArrayList<>();
        counterByValueDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(12_000L);

        assertThat(receivedData).hasSize(4);
        assertThat(receivedData).containsOnly(new Tuple2<>(makeTestLabel(0), 2L),
                new Tuple2<>(makeTestLabel(1), 2L), new Tuple2<>(makeTestLabel(2), 2L), new Tuple2<>(makeTestLabel(3), 2L));
    }

    @Test
    public void should_apply_count_by_value_when_there_are_1_rdd_per_dstream() throws IOException, InterruptedException {
        triggerDataCreation(4);
        triggerDataCreation(4);
        JavaInputDStream<String> queueDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME);

        JavaPairDStream<String, Long> counterByValueDStream = queueDStream.countByValue();

        List<Tuple2<String, Long>> receivedData = new ArrayList<>();
        counterByValueDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(12_000L);

        // countByValue(...) applies individually on RDDs contained in given DStream. In our case
        // we include 1 RDD each time. So, the RDD#1 has this format: ["Number 0"], RDD#2 this one: ["Number 1"],
        // RDD#3 this one: ["Number 2"] and so on.
        // It's the reason why, unlike in should_apply_count_by_value_when_all_RDDs_are_in_DStream, here
        // we receive a list of 8 elements.
        assertThat(receivedData).hasSize(8);
        assertThat(receivedData).containsOnly(new Tuple2<>(makeTestLabel(0), 1L),
                new Tuple2<>(makeTestLabel(1), 1L), new Tuple2<>(makeTestLabel(2), 1L), new Tuple2<>(makeTestLabel(3), 1L));

    }

    private static class LabelsConcatenator implements Function2<String, String, String>, Serializable {

        private static final String PREFIX = "Labels are: ";

        @Override
        public String call(String baseLabel, String labelToConcatenate) throws Exception {
            String prefix = baseLabel.contains(PREFIX) ? "" : PREFIX;
            return  prefix + baseLabel + ", " + labelToConcatenate;
        }
    }

    private void triggerDataCreation(int start, int maxRDDs) throws IOException {
        for (int i = start; i < maxRDDs; i++) {
            JavaRDD<String> newRDD1 = batchContext.parallelize(Arrays.asList(makeTestLabel(i)));
            testQueue.add(newRDD1);
        }
    }

    private void triggerDataCreation(int maxRDDs) throws IOException {
        triggerDataCreation(0, maxRDDs);
    }

    private static String makeTestLabel(int number) {
        return "Number " + number;
    }

    private static Integer toInt(String label) {
        return Integer.valueOf(label.replace("Number ", "").trim());
    }

}

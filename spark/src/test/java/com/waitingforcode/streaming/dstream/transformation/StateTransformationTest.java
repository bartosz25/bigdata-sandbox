package com.waitingforcode.streaming.dstream.transformation;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StateTransformationTest {

    private static final String CHECKPOINT_DIRECTORY = "/tmp/spark/checkpoint";
    private static final boolean ONE_AT_TIME = true;
    private static final long BATCH_INTERVAL =  500L;
    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("StateTransformation Test").setMaster("local[4]");
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;
    private Queue<JavaRDD<Visitor>> testQueue;

    private static final String HOME_PAGE = "home.html", ABOUT_US_PAGE = "about_us.html", CONTACT_PAGE = "contact.html";
    private static final String TEAM_PAGE = "team.html", JOB_OFFERS_PAGE = "job_offers.html", JOIN_US_PAGE = "join_us.html";
    private static final String LAST_PAGE = "close.html";


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
    public void should_fail_on_applying_update_by_state_without_checkpoint_activated() throws IOException, InterruptedException {
        // Checkpoint directory is mandatory for running *ByState() transformations.
        // Checkpoint reduces amount of metadata stored during subsequent updates (= it doesn't
        // keep traces on dependent parents).
        triggerDataCreation(0, 2, HOME_PAGE, TEAM_PAGE);

        JavaPairDStream<Integer, Visitor> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(visitor -> new Tuple2<>(visitor.id, visitor)));
        JavaPairDStream<Integer, List<String>> pagesVisitedByUserPairDStream =
                pairEntriesDStream.updateStateByKey(new AllVisitedPagesGenerator());
        pagesVisitedByUserPairDStream.foreachRDD(rdd -> System.out.println("Data: " + rdd.collect()));

        try {
            streamingContext.start();
            fail("Contenxt shouldn't be started without checkpoint directory");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage()).contains("requirement failed: The checkpoint directory has not been set");
            iae.printStackTrace();
        }
    }

    @Test
    public void should_apply_new_state_on_each_batch() throws IOException, InterruptedException {
        streamingContext.checkpoint(CHECKPOINT_DIRECTORY);
        // Imagine 2 users - each of them visits 3 pages. The first user wants to contact the company
        // and the second user wants to work for it.
        triggerDataCreation(0, 2, HOME_PAGE, TEAM_PAGE);
        triggerDataCreation(0, 2, ABOUT_US_PAGE, JOB_OFFERS_PAGE);
        triggerDataCreation(0, 2, CONTACT_PAGE, JOIN_US_PAGE);

        JavaPairDStream<Integer, Visitor> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(visitor -> new Tuple2<>(visitor.id, visitor)));

        // state = value
        JavaPairDStream<Integer, List<String>> pagesVisitedByUserPairDStream =
                pairEntriesDStream.updateStateByKey(new AllVisitedPagesGenerator());

        Map<Integer, Set<List<String>>> receivedData = new HashMap<>();
        receivedData.put(0, new HashSet<>());
        receivedData.put(1, new HashSet<>());
        pagesVisitedByUserPairDStream.foreachRDD(rdd -> {
            rdd.collectAsMap().forEach((visitorId, visitedPages) -> {
                receivedData.get(visitorId).add(visitedPages);
            });
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(3_000L);

        // User#1: home.html -> about_us.html -> contact.html
        assertThat(receivedData.get(0)).hasSize(3);
        assertThat(receivedData.get(0)).containsOnly(Arrays.asList(HOME_PAGE), Arrays.asList(HOME_PAGE, ABOUT_US_PAGE),
                Arrays.asList(HOME_PAGE, ABOUT_US_PAGE, CONTACT_PAGE));
        // User#2: team.html > job_offers.html -> join_us.html
        assertThat(receivedData.get(1)).hasSize(3);
        assertThat(receivedData.get(1)).containsOnly(Arrays.asList(TEAM_PAGE), Arrays.asList(TEAM_PAGE, JOB_OFFERS_PAGE),
                Arrays.asList(TEAM_PAGE, JOB_OFFERS_PAGE, JOIN_US_PAGE));
    }

    @Test
    public void should_eliminate_item_from_rdd_because_of_empty_next_state() throws InterruptedException, IOException {
        streamingContext.checkpoint(CHECKPOINT_DIRECTORY);
        triggerDataCreation(0, 2, HOME_PAGE, TEAM_PAGE);
        triggerDataCreation(0, 2, ABOUT_US_PAGE, LAST_PAGE);
        // User#2 closed his browser and reopened it to start new navigation
        // while User#1 still continues his browsing
        triggerDataCreation(0, 2, CONTACT_PAGE, HOME_PAGE);

        JavaPairDStream<Integer, Visitor> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(visitor -> new Tuple2<>(visitor.id, visitor)));

        // state = value
        JavaPairDStream<Integer, List<String>> pagesVisitedByUserPairDStream =
                pairEntriesDStream.updateStateByKey(new AllVisitedPagesGenerator());

        Map<Integer, Set<List<String>>> receivedData = new HashMap<>();
        receivedData.put(0, new HashSet<>());
        receivedData.put(1, new HashSet<>());
        List<Integer> partitions = new ArrayList<>();
        pagesVisitedByUserPairDStream.foreachRDD(rdd -> {
            partitions.add(rdd.getNumPartitions());
            rdd.collectAsMap().forEach((visitorId, visitedPages) -> receivedData.get(visitorId).add(visitedPages));
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(3_000L);

        // User#1: home.html -> about_us.html -> contact.html
        assertThat(receivedData.get(0)).hasSize(3);
        assertThat(receivedData.get(0)).containsOnly(Arrays.asList(HOME_PAGE), Arrays.asList(HOME_PAGE, ABOUT_US_PAGE),
                Arrays.asList(HOME_PAGE, ABOUT_US_PAGE, CONTACT_PAGE));
        // User#2: team.html > job_offers.html -> join_us.html
        assertThat(receivedData.get(1)).hasSize(2);
        assertThat(receivedData.get(1)).containsOnly(Arrays.asList(TEAM_PAGE), Arrays.asList(HOME_PAGE));
        // Check the number of partitions too
        assertThat(partitions).containsOnly(4);
    }

    @Test
    public void should_update_state_and_change_the_number_of_partitions() throws InterruptedException, IOException {
        streamingContext.checkpoint("/tmp/spark/checkpoints_streaming");
        triggerDataCreation(0, 2, HOME_PAGE, TEAM_PAGE);
        triggerDataCreation(0, 2, ABOUT_US_PAGE, JOB_OFFERS_PAGE);

        JavaPairDStream<Integer, Visitor> pairEntriesDStream = streamingContext.queueStream(testQueue, ONE_AT_TIME)
                .transformToPair(rdd -> rdd.mapToPair(visitor -> new Tuple2<>(visitor.id, visitor)));

        int partitionSize = 2;
        JavaPairDStream<Integer, List<String>> pagesVisitedByUserPairDStream =
                pairEntriesDStream.updateStateByKey(new AllVisitedPagesGenerator(), partitionSize);

        List<Integer> partitions = new ArrayList<>();
        pagesVisitedByUserPairDStream.foreachRDD(rdd -> partitions.add(rdd.getNumPartitions()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(3_000L);

        assertThat(partitions).containsOnly(partitionSize);
    }

    private void triggerDataCreation(int start, int maxRDDs, String...pages) throws IOException {
        for (int i = start; i < maxRDDs; i++) {
            JavaRDD<Visitor> newRDD1 = batchContext.parallelize(Arrays.asList(new Visitor(i, pages[i])));
            testQueue.add(newRDD1);
        }
    }

    private static class Visitor implements Serializable {
        private int id;
        private String page;

        public Visitor(int id, String page) {
            this.id = id;
            this.page = page;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("id", id)
                    .add("page", page).toString();
        }
    }

    private static class AllVisitedPagesGenerator implements Serializable, Function2<List<Visitor>, Optional<List<String>>, Optional<List<String>>> {

        @Override
        public Optional<List<String>> call(List<Visitor> values, Optional<List<String>> previousState) throws Exception {
            // values = visitors accumulated during batch duration;
            // In our case it means new visited page (because of one-at-time flag
            // set to true on queue DStream)
            List<String> visitedPages = values.stream().map(visitor -> visitor.page).collect(Collectors.toList());
            if (visitedPages.contains(LAST_PAGE)) {
                // Here we consider that user ended his browsing.
                // It's represented by LAST_PAGE's visit.
                // We don't want to include this page in updated state
                // so we return empty(). By doing that, user's browsing stops
                // here. If he's seen new time, it will be considered as a new
                // browsing.
                return Optional.empty();
            }
            // previousState can be empty when the key is updated for the first time
            List<String> alreadyVisitedPages = previousState.orElse(new ArrayList<>());
            List<String> allPages = Lists.newArrayList(alreadyVisitedPages);
            allPages.addAll(visitedPages);
            return Optional.of(allPages);
        }
    }

}

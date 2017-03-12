package com.waitingforcode.rdd;

import com.google.common.base.MoreObjects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class SharedObjectsTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("SharedObjects Test").setMaster("local[1]");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);

    private static final List<Integer> TEST_VALUES = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());

    @Test
    public void should_correctly_use_broadcast_variable() {
        CollectionAccumulator<TestClass> broadcastAccumulator =
                CONTEXT.sc().collectionAccumulator("broadcast accumulator");
        Broadcast<TestClass> largeNumber = CONTEXT.broadcast(new TestClass());
        CONTEXT.parallelize(TEST_VALUES)
                .map(number -> {
                    broadcastAccumulator.add(largeNumber.value());
                    return number;
                })
                .collect();


        List<TestClass> operatedClasses = broadcastAccumulator.value();
        for (int i = 1; i < operatedClasses.size(); i++) {
            assertThat(operatedClasses.get(i-1)).isSameAs(operatedClasses.get(i));
        }
    }


    @Test
    public void should_use_accumulator_to_count_filtered_objects() {
        LongAccumulator longAccumulator = CONTEXT.sc().longAccumulator("Filtered items counter");
        CONTEXT.parallelize(TEST_VALUES)
                .filter(number -> {
                    longAccumulator.add(1L);
                    return number < 50;
                }).collect();

        assertThat(longAccumulator.value()).isEqualTo(100);
    }

    @Test
    public void should_use_accumulator_with_custom_type() {
        List<String> urls = Arrays.asList("URL#1", "URL#2", "URL#3", "URL#4", "URL#5", "URL#6");
        CollectionAccumulator<MetricBean> metricsAccumulator = CONTEXT.sc().collectionAccumulator("metrics accumulator");

        CONTEXT.parallelize(urls).map(
                url -> {
                    Random randomizer = new Random();
                    randomizer.nextLong();
                    metricsAccumulator.add(new MetricBean(url, randomizer.nextLong()));
                    return url+ " was treated";
                }
        ).collect();

        List<MetricBean> metrics = metricsAccumulator.value();
        assertThat(metrics).hasSize(6);
        assertThat(metrics).extracting("url").containsAll(urls);
        metrics.forEach(metricBean -> assertThat(metricBean.connectionTime).isNotNull());
    }

    @Test
    public void should_fail_on_aggregating_values_without_accumulator() {
        // Executors work on local copy of the object and
        // the changes aren't propagated back to the driver
        Map<String, Integer> stats = new ConcurrentHashMap<>();
        stats.put("BIG", 0);
        stats.put("NOT_BIG", 0);

        JavaRDD<Integer> bigNumbersRDD = CONTEXT.parallelize(TEST_VALUES)
                .filter(value -> {
                    boolean isBigNumber = value > 9;
                    String key = "NOT_BIG";
                    if (isBigNumber) {
                        key = "BIG";
                    }
                    stats.replace(key, stats.get(key) + 1);
                    return isBigNumber;
                });

        assertThat(bigNumbersRDD.collect()).hasSize(91);
        assertThat(stats).hasSize(2);
        assertThat(stats.get("BIG")).isEqualTo(0);
        assertThat(stats.get("NOT_BIG")).isEqualTo(0);
    }

    private static class MetricBean implements Serializable {
        private final String url;
        private final Long connectionTime;

        public MetricBean(String url, Long connectionTime) {
            this.url = url;
            this.connectionTime = connectionTime;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("url", url)
                    .add("connectionTime", connectionTime).toString();
        }
    }

    private static class TestClass implements Serializable {
        private UUID id = UUID.randomUUID();
    }

}

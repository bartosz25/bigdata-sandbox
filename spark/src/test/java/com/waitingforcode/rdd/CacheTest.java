package com.waitingforcode.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class CacheTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Cache Test").setMaster("local[1]");

    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);

    private JavaRDD<Integer> testedNumersRDD =
            CONTEXT.parallelize(IntStream.rangeClosed(1, 5).boxed().collect(Collectors.toList()), 2);

    @Test
    public void should_correctly_cache_simple_rdd_on_disk() {
        JavaRDD<Integer> filteredRDD = testedNumersRDD.filter(number -> number > 50);

        filteredRDD.persist(StorageLevel.DISK_ONLY());

        filteredRDD.count();

        // To check if given RDD was cached, we can search CachedPartitions in debug text
        // Information about cache method (disk/memory only or both, replicated or not etc)
        // can be found in '[]', at the end of each line, as for example here:
        // MapPartitionsRDD[1] at filter at CacheTest.java:26 [Disk Serialized 1x Replicated]
        assertThat(filteredRDD.toDebugString()).contains("CachedPartitions: 2; MemorySize: 0.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 8.0 B",
                "[Disk Serialized 1x Replicated]");
    }

    @Test
    public void should_unpersist_rdd_cached_on_disk() {
        JavaRDD<Integer> filteredRDD = testedNumersRDD.filter(number -> number > 50);

        filteredRDD.persist(StorageLevel.DISK_ONLY());

        filteredRDD.count();

        // To check if given RDD was cached, we can search CachedPartitions in debug text
        // Information about cache method (disk/memory only or both, replicated or not etc)
        // can be found in '[]', at the end of each line, as for example here:
        // MapPartitionsRDD[1] at filter at CacheTest.java:26 [Disk Serialized 1x Replicated]
        assertThat(filteredRDD.toDebugString()).contains("CachedPartitions: 2; MemorySize: 0.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 8.0 B",
                "[Disk Serialized 1x Replicated]");

        // When RDD is unpersisted, we can see that in logs, more particularly by
        // looking for following entries:
        // <pre>
        // INFO Removing RDD 1 from persistence list (org.apache.spark.rdd.MapPartitionsRDD:54)
        // DEBUG removing RDD 1 (org.apache.spark.storage.BlockManagerSlaveEndpoint:58)
        // </pre>
        // This is a blocking operation. It stops thread until all blocks
        // are removed from cache.
        filteredRDD.unpersist();

        assertThat(filteredRDD.toDebugString()).doesNotContain("CachedPartitions: 2; MemorySize: 0.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 8.0 B");
        assertThat(filteredRDD.toDebugString()).doesNotContain("[Disk Serialized 1x Replicated]");
    }

    @Test
    public void should_correctly_cache_simple_rdd_on_disk_with_replicates() {
        JavaRDD<Integer> filteredRDD = testedNumersRDD.filter(number -> number > 50);

        filteredRDD.persist(StorageLevel.DISK_ONLY_2());

        filteredRDD.count();

        // To check if given RDD was cached, we can search CachedPartitions in debug text
        // Information about cache method (disk/memory only or both, replicated or not etc)
        // can be found in '[]', at the end of each line, as for example here:
        // MapPartitionsRDD[1] at filter at CacheTest.java:26 [Disk Serialized 1x Replicated]
        assertThat(filteredRDD.toDebugString()).contains("CachedPartitions: 2; MemorySize: 0.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 8.0 B",
                "[Disk Serialized 1x Replicated]");
    }

    @Test
    public void should_correctly_cache_rdd_with_custom_storage_level() {
        JavaRDD<Integer> filteredRDD = testedNumersRDD.filter(number -> number > 50);

        // We try to create a storage level for deserializable and disk
        // which doesn't exist (partitions are serialized on disk)
        boolean useDisk = true, useMemory = false, deserialized = true, useOffHeap = false;
        int replicationFactor = 1;
        filteredRDD.persist(StorageLevel.apply(useDisk, useMemory, useOffHeap, deserialized, replicationFactor));

        filteredRDD.collect();

        // To check if given RDD was cached, we can search CachedPartitions in debug text
        // Information about cache method (disk/memory only or both, replicated or not etc)
        // can be found in '[]', at the end of each line, as for example here:
        // MapPartitionsRDD[1] at filter at CacheTest.java:26 [Disk Serialized 1x Replicated]
        assertThat(filteredRDD.toDebugString()).contains("CachedPartitions: 2; MemorySize: 0.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 8.0 B",
                "[Disk Deserialized 1x Replicated]");
    }

    @Test
    public void should_correctly_cache_simple_rdd_on_memory_only() {
        JavaRDD<Integer> filteredRDD = testedNumersRDD.filter(number -> number > 50);

        // cache() corresponds to the use of persist() which
        // calls persist(StorageLevel.MEMORY_ONLY)
        filteredRDD.cache();

        // action to execute transformations
        filteredRDD.count();

        // To check if given RDD was cached, we can search CachedPartitions in debug text
        // Information about cache method (disk/memory only or both, replicated or not etc)
        // can be found in '[]', at the end of each line, as for example here:
        // ParallelCollectionRDD[0] at parallelize at CacheTest.java:19 [Memory Deserialized 1x Replicated]
        assertThat(filteredRDD.toDebugString()).contains("CachedPartitions: 2; MemorySize: 32.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B",
                "[Memory Deserialized 1x Replicated]");

    }

}

package com.waitingforcode.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

import static java.util.Collections.singletonList;

public final class CollectionHelper {

    private CollectionHelper() {
        // prevents init
    }


    public static <T> void putIfDefined(List<T> collectedData, List<List<T>> windows) {
        if (!collectedData.isEmpty()) {
            windows.add(collectedData);
        }
    }

    public static  void triggerDataCreation(int start, int maxRDDs,
                                            JavaSparkContext sparkContext, Queue<JavaRDD<Integer>> testQueue) throws IOException {
        for (int i = start; i < maxRDDs; i++) {
            JavaRDD<Integer> newRDD1 = sparkContext.parallelize(singletonList(i));
            testQueue.add(newRDD1);
        }
    }

}

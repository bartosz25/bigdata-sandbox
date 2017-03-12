package com.waitingforcode.shuffle;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ShuffleTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Shuffle Test").setMaster("local[1]")
                    .set("spark.shuffle.manager", "sort")
                    .set("spark.ui.enabled", "false")
                    .set("spark.shuffle.compress", "false")
                    .set("spark.cores.max", "1")
                    .set("spark.shuffle.file.buffer", "48k")
                    .set("spark.reducer.maxSizeInFlight", "1m")
                    .set("spark.shuffle.sort.bypassMergeThreshold", "132");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);

    @Test
    public void should_detect_shuffle_with_used_manager() {
        Random random = new Random();
        List<Tuple2<Integer, Integer>> tuples = new ArrayList<>();
        for (int i = 0; i < 550_000; i++) {
            tuples.add(new Tuple2<>(random.nextInt(100), random.nextInt(100)));
        }

        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = CONTEXT.parallelizePairs(tuples);

        JavaPairRDD<String, Integer> tuple2JavaRDD = integerIntegerJavaPairRDD.mapToPair(new Mapper());

        JavaPairRDD<BigDecimal, Integer> bigDecimalIntegerJavaPairRDD =
                tuple2JavaRDD.mapToPair(tuple -> new Tuple2<>(BigDecimal.valueOf(tuple._2()), tuple._2()));
        JavaPairRDD<BigDecimal, Integer> repartition = bigDecimalIntegerJavaPairRDD.repartition(5);

        Map<BigDecimal, Long> reducedPairs = repartition.countByKey(); //((v1, v2) -> v1 + v2);

//bigDecimalIntegerJavaPairRDD.sortByKey().mapToPair(tuple -> new Tuple2<>(tuple._2()%2, tuple._2())).collectAsMap();

        System.out.println("repartition= "+ repartition.toDebugString());
        //System.out.println("reducedPairs= "+ tuple2JavaRDD.collect());

        String[] datas = new String[]{"/tmp/shuffle_2_cores/0f/shuffle_0_1_0.index", "/tmp/shuffle_2_cores/30/shuffle_0_0_0.index"};
        for (String dataFileName : datas) {
            System.out.println("For data file " + dataFileName);
            File dataFile = new File(dataFileName);
            Serializer serializer = CONTEXT.env().serializer();
            try {
                InputStream data = FileUtils.openInputStream(dataFile);
                System.out.println("> " + IOUtils.toCharArray(data, "UTF-8"));

                DeserializationStream deserializationStream = serializer.newInstance().deserializeStream(data);
                Iterator<Object> objectIterator = deserializationStream.asIterator();
                while (objectIterator.hasNext()) {
                    System.out.println("next = " + objectIterator.next());
                }
                Iterator<Tuple2<Object, Object>> tuple2Iterator = deserializationStream.asKeyValueIterator();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<Object, Object> n = tuple2Iterator.next();
                    System.out.println("n= " + n);
                }
                System.out.println(">>>>>>>>>>>>>>>>< " + deserializationStream.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
         /*
        serializer = CONTEXT.env().serializer();
        File indexFile = new File("/tmp/shuffle_copy/30/shuffle_0_0_0.index");
        try {
            InputStream data = FileUtils.openInputStream(indexFile);
            DeserializationStream deserializationStream = serializer.newInstance().deserializeStream(data);
            Iterator<Object> iterator = deserializationStream.asIterator();
            while (iterator.hasNext()) {
                Object  n = iterator.next();
                System.out.println("n= "+ n);
            }
            System.out.println(">>>>>>>>>>>>>>>>< " + deserializationStream.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }    */
    }

    public static class Mapper implements PairFunction<Tuple2<Integer, Integer>, String, Integer>, Serializable {

        @Override
        public Tuple2<String, Integer> call(Tuple2<Integer, Integer> tuple) throws Exception {
            return new Tuple2<>(getSizeLabel(tuple._1()), tuple._2());
        }

    }

    public static String getSizeLabel(int number) {
        Map<Integer, String> labels = new HashMap<>();
        labels.put(0, "[0-10]");
        labels.put(1, "[11-20]");
        labels.put(2, "[21-30]");
        labels.put(3, "[31-40]");
        labels.put(4, "[41-50]");
        labels.put(5, "[51-60]");
        labels.put(6, "[61-70]");
        labels.put(7, "[71-80]");
        labels.put(8, "[81-90]");
        labels.put(9, "[91-100]");
        for (int i = 0; i*10 < 100; i++) {
            if (number < i) {
                return labels.get(i);
            }
        }
        return "UKNOWN";
    }

}

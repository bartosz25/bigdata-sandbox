package com.waitingforcode.sql;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

public class PlanDebug {

    private static final SparkSession SESSION = SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp/spark")
            .appName("Dataset Test").getOrCreate();

    @Test
    public void should_generate_physical_plan() {
        AgedPerson youngster = AgedPerson.of("young", 17);
        AgedPerson older = AgedPerson.of("older", 300);
        Dataset<Row> dataset = SESSION.createDataFrame(Arrays.asList(youngster, older), AgedPerson.class);

        Dataset<Row> people = dataset.filter(new MinAgeFilter(20));
        people.foreach(rdd -> {});
    }

    public static class AgedPerson {
        private String name;

        private int age;

        public static AgedPerson of(String name, int age) {
            AgedPerson agedPerson = new AgedPerson();
            agedPerson.age = age;
            agedPerson.name = name;
            return agedPerson;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static class MinAgeFilter implements FilterFunction<Row>, Serializable {

        private final int minAge;

        public MinAgeFilter(int minAge) {
            this.minAge = minAge;
        }

        @Override
        public boolean call(Row row) throws Exception {
            return row.<Integer>getAs("age") >= minAge;
        }
    }


}

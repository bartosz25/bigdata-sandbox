package com.waitingforcode.sql;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetTransformationTest {

    private static final SparkSession SESSION = SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp/spark")
            .appName("DatasetTransformation Test").getOrCreate();


    @Test
    public void should_filter_and_order_dataset() {
        String filePath = getClass().getClassLoader().getResource("dataset/cities.json").getPath();

        Dataset<Row> cities = SESSION.read().json(filePath);

        Encoder<DatasetTest.City> cityEncoder = Encoders.bean(DatasetTest.City.class);
        List<DatasetTest.City> europeanCities = cities.filter((Row cityRow) -> cityRow.<String>getAs("continent").equals("Europe"))
                .orderBy(new Column("name").asc())
                .map((MapFunction<Row, DatasetTest.City>) value -> DatasetTest.City.fromRow(value), cityEncoder)
                .collectAsList();

        assertThat(europeanCities).hasSize(3);
        assertThat(europeanCities).extracting("name").containsExactly("Paris", "Rome", "Warsaw");
        assertThat(europeanCities).extracting("country").containsExactly("FR", "IT", "PL");
        assertThat(europeanCities).extracting("continent").containsExactly("Europe", "Europe", "Europe");
    }

    @Test
    public void should_filter_and_order_dataset_with_string_expressions() {
        String filePath = getClass().getClassLoader().getResource("dataset/cities.json").getPath();

        Dataset<Row> cities = SESSION.read().json(filePath);

        Encoder<DatasetTest.City> cityEncoder = Encoders.bean(DatasetTest.City.class);
        List<DatasetTest.City> europeanCities = cities.filter("continent = 'Europe'")
                .sort("name")
                .map((MapFunction<Row, DatasetTest.City>) value -> DatasetTest.City.fromRow(value), cityEncoder)
                .collectAsList();

        assertThat(europeanCities).hasSize(3);
        assertThat(europeanCities).extracting("name").containsExactly("Paris", "Rome", "Warsaw");
        assertThat(europeanCities).extracting("country").containsExactly("FR", "IT", "PL");
        assertThat(europeanCities).extracting("continent").containsExactly("Europe", "Europe", "Europe");
    }

    @Test
    public void should_union_two_datasets() {
        // We can execute similar operations on Datasets as on RDDs
        // One of them is union
        String filePath = getClass().getClassLoader().getResource("dataset/cities.json").getPath();
        String filePathUs = getClass().getClassLoader().getResource("dataset/cities_us.json").getPath();

        Dataset<Row> americanCities = SESSION.read().json(filePathUs);
        Dataset<Row> restOfWorldCities = SESSION.read().json(filePath);

        Encoder<DatasetTest.City> cityEncoder = Encoders.bean(DatasetTest.City.class);
        List<DatasetTest.City> cities = americanCities.union(restOfWorldCities)
                .map((MapFunction<Row, DatasetTest.City>) value -> DatasetTest.City.fromRow(value), cityEncoder)
                .orderBy(new Column("name").asc())
                .collectAsList();

        assertThat(cities).hasSize(8);
        assertThat(cities).extracting("name").containsExactly("Boston", "Buenos Aires", "Chicago",
                "New Jersey", "New York", "Paris", "Rome", "Warsaw");
        assertThat(cities).extracting("country").containsExactly("US", "AR", "US", "US", "US", "FR", "IT", "PL");
        assertThat(cities).extracting("continent").containsExactly("North America", "South America", "North America",
                "North America", "North America", "Europe", "Europe", "Europe");
    }
}

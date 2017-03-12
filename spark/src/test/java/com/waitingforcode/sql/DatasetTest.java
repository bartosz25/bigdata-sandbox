package com.waitingforcode.sql;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static com.waitingforcode.sql.DatasetTest.City.fromRow;
import static com.waitingforcode.sql.DatasetTest.City.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class DatasetTest {

    private static final SparkSession SESSION = SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp/spark")
            .appName("Dataset Test").getOrCreate();


    @Test
    public void should_construct_dataset_from_json() {
        // cities.json is not correctly formatted JSON. It's because
        // .json(String) reads objects line per line which is shown
        // in should_construct_bad_dataset_from_correctly_formatted_json test
        Dataset<Row> cities = readCitiesFromJson();

        assertThat(cities.collectAsList()).hasSize(4);
    }

    @Test
    public void should_construct_bad_dataset_from_correctly_formatted_json() {
        String filePath = getClass().getClassLoader().getResource("dataset/cities_correct_format.json").getPath();

        Dataset<Row> cities = SESSION.read().json(filePath);

        // It's 8 because .json(String) takes 1 row per line, even if the row doesn't
        // contain defined object (city)
        assertThat(cities.collectAsList()).hasSize(8);
        cities.printSchema();
    }

    @Test
    public void should_create_dataset_from_rdd() {
        JavaSparkContext closeableContext = new JavaSparkContext(SESSION.sparkContext());
        try {
            List<String> textList = Lists.newArrayList("Txt1", "Txt2");
            JavaRDD<Row> textRdd = closeableContext.parallelize(textList)
                    .map((Function<String, Row>) record -> RowFactory.create(record));

            StructType schema = DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("content", DataTypes.StringType, true)
            });
            Dataset<Row> datasetFromRdd = SESSION.createDataFrame(textRdd, schema);

            assertThat(datasetFromRdd.count()).isEqualTo(2L);
            List<Row> rows = datasetFromRdd.collectAsList();
            assertThat(rows.get(0).mkString()).isEqualTo(textList.get(0));
            assertThat(rows.get(1).mkString()).isEqualTo(textList.get(1));
        } finally {
            closeableContext.close();
        }
    }

    @Test
    public void should_fail_on_creating_create_dataset_from_rdd_with_incorrect_schema() {
        JavaSparkContext closeableContext = new JavaSparkContext(SESSION.sparkContext());
        try {
            List<String> textList = Lists.newArrayList("Txt1", "Txt2");
            JavaRDD<Row> textRdd = closeableContext.parallelize(textList)
                    .map((Function<String, Row>) record -> RowFactory.create(record));

            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("content", DataTypes.TimestampType, true)
            });
            Dataset<Row> datasetFromRdd = SESSION.createDataFrame(textRdd, schema);

            // Trigger action to make Dataset really created
            assertThatExceptionOfType(Exception.class).isThrownBy(() -> datasetFromRdd.show())
                    .withMessageContaining("java.lang.String is not a valid external type for schema of timestamp");
        } finally {
            closeableContext.close();
        }
    }

    @Test
    public void should_print_dataset_schema() {
        Dataset<Row> cities = readCitiesFromJson();

        cities.printSchema();
        // Schema should look like:
        // root
        // |-- continent: string (nullable = true)
        // |-- country: string (nullable = true)
        // |-- name: string (nullable = true)
        // Here also it's difficult to test, so check simply if Dataset
        // was constructed

        assertThat(cities).isNotNull();
    }

    @Test
    public void should_get_rdd_from_dataset() {
        Dataset<Row> cities = readCitiesFromJson();

        // Dataset stores collected data as RDD
        // which is a Dataset's field access in Java from rdd()
        // method
        RDD<Row> scalaRdd = cities.rdd();

        assertThat(scalaRdd).isNotNull();
        String rddDag = scalaRdd.toDebugString();
        assertThat(rddDag).contains("FileScanRDD[3]", "MapPartitionsRDD[4]", "MapPartitionsRDD[5]", "MapPartitionsRDD[6]");
        // We could also use cities.toJavaRDD()
        JavaRDD<Row> javaRdd = scalaRdd.toJavaRDD();
        List<City> citiesList = javaRdd.map(row -> fromRow(row)).collect();
        assertThat(citiesList).hasSize(4);
        assertThat(citiesList).extracting("name").containsOnly("Paris", "Warsaw", "Rome", "Buenos Aires");
    }

    @Test
    public void should_create_dataset_from_objects() throws InterruptedException {
        City london = valueOf("London", "England", "Europe");
        City paris = valueOf("Paris", "France", "Europe");
        City warsaw = valueOf("Warsaw", "Poland", "Europe");
        City tokio = valueOf("Tokio", "Japan", "Asia");

        Encoder<City> cityEncoder = Encoders.bean(City.class);
        Dataset<City> dataset = SESSION.createDataset(Arrays.asList(london, paris, warsaw, tokio), cityEncoder);

        assertThat(dataset.count()).isEqualTo(4);
    }

    @Test
    public void should_get_dataset_from_database() {
        InMemoryDatabase.init();

        Dataset<Row> dataset = SESSION.read()
                .format("jdbc")
                .option("url", InMemoryDatabase.DB_CONNECTION)
                .option("driver", InMemoryDatabase.DB_DRIVER)
                .option("dbtable", "city")
                .option("user", InMemoryDatabase.DB_USER)
                .option("password", InMemoryDatabase.DB_PASSWORD)
                .load();

        assertThat(dataset.count()).isEqualTo(7);
    }

    @Test
    public void should_filter_dataset_from_database() {
        InMemoryDatabase.init();

        Dataset<Row> dataset = SESSION.read()
                .format("jdbc")
                .option("url", InMemoryDatabase.DB_CONNECTION)
                .option("driver", InMemoryDatabase.DB_DRIVER)
                .option("dbtable", "city")
                .option("user", InMemoryDatabase.DB_USER)
                .option("password", InMemoryDatabase.DB_PASSWORD)
                .load();
        // NAME is in upper case instead of lower case
        Dataset<Row> twoLetterCitiesDataset = dataset.filter((Row cityRow) -> cityRow.<String>getAs("NAME").length() > 2);

        assertThat(twoLetterCitiesDataset.count()).isEqualTo(7);
    }

    @Test
    public void should_show_how_physical_plan_is_planed() {
        // Datasets are strictly related to Catalyst Optimizer described
        // in another post
        Dataset<Row> cities = readCitiesFromJson();

        Dataset<Row> europeanCities = cities
                .filter((Row cityRow) -> cityRow.<String>getAs("continent").equals("Europe"))
                .orderBy(new Column("name").asc())
                .filter((Row cityRow) -> cityRow.<String>getAs("name").startsWith("ABC"))
                .limit(1);

        europeanCities.explain();
        // It's quite impossible to test physical plan explanation
        // It's the reason why we test the result and give physical plan output as
        // a comment:
        // == Physical Plan ==
        // CollectLimit 1
        // +- *Filter com.waitingforcode.sql.DatasetTest$$Lambda$10/295937119@69796bd0.call
        //    +- *Sort [name#2 ASC NULLS FIRST], true, 0
        //       +- Exchange rangepartitioning(name#2 ASC NULLS FIRST, 200)
        //          +- *Filter com.waitingforcode.sql.DatasetTest$$Lambda$9/907858780@69ed5ea2.call
        //             +- *FileScan json [continent#0,country#1,name#2] Batched: false, Format: JSON, Location:
        //                 InMemoryFileIndex[file:sandbox/spark/target/test-classes/dataset..., PartitionFilters:
        //                 [], PushedFilters: [], ReadSchema: struct<continent:string,country:string,name:string>
        // As you can see, the physical plan started with file reading, without any
        // pushdown predicate. After that both filters were applied
        // together.
        europeanCities.foreach(r -> {});
        //assertThat(europeanCities.count()).isEqualTo(1L);
    }

    @Test
    public void should_show_logical_and_physical_plan_phases() {
        Dataset<Row> cities = readCitiesFromJson();

        Dataset<Row> europeanCities = cities.filter((Row cityRow) -> cityRow.<String>getAs("continent").equals("Europe"))
                .orderBy(new Column("name").asc())
                .filter((Row cityRow) -> cityRow.<String>getAs("name").length() > 2)
                .limit(1);

        boolean withLogicalPlan = true;
        europeanCities.explain(withLogicalPlan);

        // As should_show_how_physical_plan_is_planed, it's difficult to test printed output
        // It's the reason why the output is printed here and the test concerns returned elements
        // == Parsed Logical Plan ==
        // GlobalLimit 1
        // +- LocalLimit 1
        //   +- TypedFilter com.waitingforcode.sql.DatasetTest$$Lambda$10/295937119@69796bd0,
        //       interface org.apache.spark.sql.Row, [StructField(continent,StringType,true),
        //       StructField(country,StringType,true), StructField(name,StringType,true)],
        //       createexternalrow(continent#0.toString, country#1.toString, name#2.toString,
        //       StructField(continent,StringType,true), StructField(country,StringType,true),
        //       StructField(name,StringType,true))
        //      +- Sort [name#2 ASC NULLS FIRST], true
        //        +- TypedFilter com.waitingforcode.sql.DatasetTest$$Lambda$9/907858780@69ed5ea2,
        //           interface org.apache.spark.sql.Row, [StructField(continent,StringType,true),
        //           StructField(country,StringType,true), StructField(name,StringType,true)],
        //           createexternalrow(continent#0.toString, country#1.toString, name#2.toString,
        //           StructField(continent,StringType,true), StructField(country,StringType,true),
        //           StructField(name,StringType,true))
        //           +- Relation[continent#0,country#1,name#2] json

        // == Analyzed Logical Plan ==
        // continent: string, country: string, name: string
        // GlobalLimit 1
        // +- LocalLimit 1
        //   +- TypedFilter com.waitingforcode.sql.DatasetTest$$Lambda$10/295937119@69796bd0,
        //       interface org.apache.spark.sql.Row, [StructField(continent,StringType,true),
        //       StructField(country,StringType,true), StructField(name,StringType,true)],
        //       createexternalrow(continent#0.toString, country#1.toString, name#2.toString,
        //       StructField(continent,StringType,true), StructField(country,StringType,true),
        //       StructField(name,StringType,true))
        //     +- Sort [name#2 ASC NULLS FIRST], true
        //       +- TypedFilter com.waitingforcode.sql.DatasetTest$$Lambda$9/907858780@69ed5ea2,
        //          interface org.apache.spark.sql.Row, [StructField(continent,StringType,true),
        //          StructField(country,StringType,true), StructField(name,StringType,true)],
        //          createexternalrow(continent#0.toString, country#1.toString, name#2.toString,
        //          StructField(continent,StringType,true), StructField(country,StringType,true),
        //          StructField(name,StringType,true))
        //         +- Relation[continent#0,country#1,name#2] json
        // Analyzed logical plan is a plan with resolved relations and attributes.
        // The resolution is made with analyzer. It works only on still unresolved
        // attributes and relations. Unlike parsed plan, we can see that row attributes
        // (continent, country, name) are defined and type.

        // == Optimized Logical Plan ==
        // GlobalLimit 1
        // +- LocalLimit 1
        //   +- TypedFilter com.waitingforcode.sql.DatasetTest$$Lambda$10/295937119@69796bd0, interface org.apache.spark.sql.Row, [StructField(continent,StringType,true), StructField(country,StringType,true), StructField(name,StringType,true)], createexternalrow(continent#0.toString, country#1.toString, name#2.toString, StructField(continent,StringType,true), StructField(country,StringType,true), StructField(name,StringType,true))
        //     +- Sort [name#2 ASC NULLS FIRST], true
        //       +- TypedFilter com.waitingforcode.sql.DatasetTest$$Lambda$9/907858780@69ed5ea2, interface org.apache.spark.sql.Row, [StructField(continent,StringType,true), StructField(country,StringType,true), StructField(name,StringType,true)], createexternalrow(continent#0.toString, country#1.toString, name#2.toString, StructField(continent,StringType,true), StructField(country,StringType,true), StructField(name,StringType,true))
        //         +- Relation[continent#0,country#1,name#2] json
        // Represents optimized logical plan, ie. a logical plan
        // on which rule-based optimizations were applied

        // == Physical Plan ==
        // ...
        // And finally physical plan is the same as in should_show_how_physical_plan_is_planed
        // It's omitted here for brevity
        assertThat(europeanCities.count()).isEqualTo(1L);
    }

    private Dataset<Row> readCitiesFromJson() {
        String filePath = getClass().getClassLoader().getResource("dataset/cities.json").getPath();
        return SESSION.read().json(filePath);
    }

    public static class City implements Serializable {
        private String name;

        private String country;

        private String continent;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getContinent() {
            return continent;
        }

        public void setContinent(String continent) {
            this.continent = continent;
        }

        public static City fromRow(Row row) {
            City city = new City();
            city.name = row.getAs("name");
            city.country = row.getAs("country");
            city.continent = row.getAs("continent");
            return city;
        }

        public static City valueOf(String name, String country, String continent) {
            City city = new City();
            city.name = name;
            city.country = country;
            city.continent = continent;
            return city;
        }

    }

}

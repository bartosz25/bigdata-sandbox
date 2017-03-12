package com.waitingforcode.sql.schema;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class SchemaTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Schema Test").setMaster("local[1]");
    private static final JavaSparkContext SPARK_CONTEXT = new JavaSparkContext(CONFIGURATION);
    private static final SparkSession SESSION = new SparkSession(SPARK_CONTEXT.sc());

    @Test
    public void should_fail_on_reading_object_with_inconsistent_schema() {
        List<Integer> ages = Lists.newArrayList(1, 2, 3, 4, 5, 6);
        JavaRDD<Row> agesRdd = SPARK_CONTEXT.parallelize(ages)
                .map(RowFactory::create);

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.BooleanType, true),
        });
        Dataset<Row> datasetFromRdd = SESSION.createDataFrame(agesRdd, schema);

        assertThatExceptionOfType(SparkException.class).isThrownBy(() -> datasetFromRdd.collectAsList())
                .withMessageContaining("java.lang.Integer is not a valid external type for schema of boolean");
    }

    @Test
    public void should_correctly_read_objects_with_consistent_schema() {
        List<Integer> ages = Lists.newArrayList(1, 2, 3, 4, 5, 6);
        JavaRDD<Row> agesRdd = SPARK_CONTEXT.parallelize(ages)
                .map(RowFactory::create);

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.IntegerType, true),
        });
        Dataset<Row> datasetFromRdd = SESSION.createDataFrame(agesRdd, schema);

        List<Row> rows = datasetFromRdd.collectAsList();
        assertThat(rows).hasSize(6);
        List<Integer> numbers = rows.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
        assertThat(numbers).containsOnly(1, 2, 3, 4, 5, 6);
    }

    @Test
    public void should_correctly_read_object_with_consistent_schema() {
        City paris = City.valueOf("Paris", 75);
        City marseille = City.valueOf("Marseille", 13);

        Dataset<Row> cities = SESSION.createDataFrame(Arrays.asList(paris, marseille), City.class);

        assertThat(cities.count()).isEqualTo(2);
    }

    @Test
    public void should_create_dataset_from_encoder() {
        City paris = City.valueOf("Paris", 75);
        City lille = City.valueOf("Lille", 59);

        Encoder<City> cityEncoder = Encoders.bean(City.class);
        Dataset<City> dataset = SESSION.createDataset(Arrays.asList(paris, lille), cityEncoder);

        assertThat(dataset.count()).isEqualTo(2);
    }

    @Test
    public void should_auto_detect_schema() {
        String filePath = getClass().getClassLoader().getResource("dataset/cities_department.json").getPath();
        Dataset<Row> citiesJson = SESSION.read().json(filePath);

        assertThat(citiesJson.count()).isEqualTo(2);
    }

    @Test
    public void should_merge_2_schemas() {
        // Below file contains cities in 2 formats: {name[String], departmentNumber[int]} and {name[String],
        // country[String], continent[String]}
        String filePath = getClass().getClassLoader().getResource("dataset/cities_merge.json").getPath();

        Dataset<Row> mergedCities = SESSION.read().option("mergeSchema", true).json(filePath);

        StructType schema = mergedCities.schema();
        scala.collection.immutable.List<StructField> fields = schema.toList();
        assertThat(fields.size()).isEqualTo(4);
        StructField nameField = fields.apply(schema.getFieldIndex("name").get());
        assertThat(nameField.dataType()).isEqualTo(DataTypes.StringType);
        StructField departmentNumberField = fields.apply(schema.getFieldIndex("departmentNumber").get());
        assertThat(departmentNumberField.dataType()).isEqualTo(DataTypes.LongType);
        StructField countryField = fields.apply(schema.getFieldIndex("country").get());
        assertThat(countryField.dataType()).isEqualTo(DataTypes.StringType);
        StructField continentField = fields.apply(schema.getFieldIndex("continent").get());
        assertThat(continentField.dataType()).isEqualTo(DataTypes.StringType);
    }

}

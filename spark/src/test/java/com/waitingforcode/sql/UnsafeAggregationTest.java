package com.waitingforcode.sql;

import org.apache.spark.sql.*;

import static java.util.Arrays.asList;

public class UnsafeAggregationTest {

    private static final SparkSession SESSION = SparkSession.builder()
            .master("local[1]")
            .appName("Unsafe aggregation").getOrCreate();

    public static void main(String[] args) {
        Employee employeeA = Employee.valueOf("A", 30, 30_000);
        Employee employeeB = Employee.valueOf("B", 25, 21_000);
        Employee employeeC = Employee.valueOf("C", 44, 41_000);
        Employee employeeD = Employee.valueOf("D", 39, 35_000);
        Employee employeeE = Employee.valueOf("E", 25, 35_000);
        Employee employeeF = Employee.valueOf("F", 30, 28_000);

        Dataset<Employee> dataset = SESSION.createDataset(asList(employeeA, employeeB, employeeC, employeeD, employeeE,
                employeeF), Encoders.bean(Employee.class));

        Dataset<Row> people = dataset.groupBy("age").agg(functions.sumDistinct("age"));
        people.foreach(rdd -> {});
    }

}

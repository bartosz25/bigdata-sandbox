package com.waitingforcode.sql;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CatalystOptimizerTest {

    private static final SparkSession SESSION = SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp")
            .appName("CatalystOptimizer Test").getOrCreate();

    @Test
    public void should_get_dataframe_from_database() {
        // categories as 18 entries
        Dataset<Row> dataset = getBaseDataset("categories");

        Dataset<Row> filteredDataset = dataset.where("LENGTH(name) > 5").where("name != 'mushrooms'")
                .limit(3);

        // To see logical plan, filteredDataset.logicalPlan() methods can be used,
        // such as: treeString(true), asCode()
        // To see full execution details, fileteredDataset.explain(true)
        // should be called
        assertThat(filteredDataset.count()).isEqualTo(3);
    }

    @Test
    public void should_get_data_from_bigger_table() {
        Dataset<Row> dataset = getBaseDataset("meals");

        Dataset<Row> filteredDataset = dataset.where("LENGTH(name) > 5")
                .where("name != 'mushrooms'")
                .where("name NOT LIKE 'Ser%'")
                .orderBy(new Column("name").desc())
                .limit(3);

        filteredDataset.show();
        assertThat(filteredDataset.count()).isEqualTo(3);
    }

    private Dataset<Row> getBaseDataset(String dbTable) {
        // Please note that previous query won't generate real SQL query. It will only
        // check if specified column exists. It can be observed with RDBMS query logs.
        // For the case of MySQL, below query is generated:
        // SELECT * FROM meals WHERE 1=0
        // Only the action (as filteredDataset.show()) will execute the query on database.
        // It also can be checked with query logs.
        return SESSION.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/fooder")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", dbTable)
                .option("user", "root")
                .option("password", "")
                .load();
    }

}

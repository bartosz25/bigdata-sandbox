package com.waitingforcode.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.waitingforcode.TestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class DeleteTest {

    @BeforeClass
    public static void initTables() throws IOException, URISyntaxException {
        SESSION.execute("CREATE KEYSPACE IF NOT EXISTS deleteTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE deleteTest");
        String tableQuery = TestHelper.readFromFile("/queries/create_simple_team_player.cql");
        System.out.println("Executing query to create table: "+tableQuery);
        SESSION.execute(tableQuery);
    }

    @AfterClass
    public static void cleanTables() {
        SESSION.execute("DROP KEYSPACE IF EXISTS deleteTest");
    }

    @Test(expected = SyntaxError.class)
    public void should_not_delete_all_data_at_once() {
        insertTeam("X", 1990, "FR", 1);
        ResultSet resultSet = SESSION.execute("SELECT * FROM simple_team");
        assertThat(resultSet.all()).isNotEmpty();

        // Deleting all rows is not possible without WHERE clause
        SESSION.execute("DELETE FROM simple_team");
    }

    @Test(expected = InvalidQueryException.class)
    public void should_not_allow_the_delete_on_where_clause_which_is_not_a_key() {
        insertTeam("X", 1990, "FR", 1);
        insertTeam("Y", 1990, "DE", 1);
        insertTeam("Z", 1990, "PL", 1);
        ResultSet resultSet = SESSION.execute("SELECT * FROM simple_team");
        assertThat(resultSet.all()).isNotEmpty();

        // Deleting all rows is not possible without WHERE clause applied on
        // partition key
        SESSION.execute("DELETE FROM simple_team WHERE division = 1");
    }

    @Test
    public void should_correctly_remove_row_by_partition_key() {
        insertTeam("X", 1990, "FR", 1);
        insertTeam("Y", 1990, "DE", 1);
        insertTeam("Z", 1990, "PL", 1);
        ResultSet resultSet = SESSION.execute("SELECT * FROM simple_team");
        assertThat(resultSet.all()).isNotEmpty();

        // Deleting all rows is not possible without WHERE clause applied on
        // partition key
        SESSION.execute("DELETE FROM simple_team WHERE teamName IN ?", Lists.newArrayList("X", "Y", "Z"));

        resultSet = SESSION.execute("SELECT * FROM simple_team");
        assertThat(resultSet.all()).isEmpty();
    }

    @Test
    public void should_remove_only_column() {
        insertTeam("A", 1990, "FR", 1);
        ResultSet resultSet = SESSION.execute("SELECT * FROM simple_team");
        assertThat(resultSet.all()).isNotEmpty();

        // Once again, only columns can be removed, no keys
        // Without specifying all keys in WHERE clause, it's not possible to remove
        // a column. This exception is returned by Cassandra when all of 3 keys are not
        // used:
        // com.datastax.driver.core.exceptions.InvalidQueryException:
        // Range deletions are not supported for specific columns
        // It's not a problem when we try to delete whole row (we can use only partition key).
        SESSION.execute("DELETE division FROM simple_team WHERE teamName = ? AND foundationYear = ? AND country = ?",
                "A", 1990, "FR");

        resultSet = SESSION.execute("SELECT * FROM simple_team WHERE teamName = ?", "A");
        Row row = resultSet.one();
        assertThat(row.get("division", Integer.class)).isNull();
        assertThat(row.getString("teamName")).isEqualTo("A");
    }

    @Test
    public void should_delete_obsolete_data_using_timestamp_clause() throws InterruptedException {
        insertTeam("A", 1990, "FR", 1);
        insertTeam("A", 1991, "FR", 1);
        insertTeam("A", 1992, "FR", 1);
        long timestamp = System.currentTimeMillis();
        Thread.sleep(2000);
        insertTeam("A", 1993, "FR", 1);
        insertTeam("A", 1994, "FR", 1);

        ResultSet resultSet = SESSION.execute("SELECT * FROM simple_team");
        assertThat(resultSet.all()).hasSize(5);

        SESSION.execute("DELETE FROM simple_team USING TIMESTAMP ? WHERE teamName = ?", timestamp, "A");

        resultSet = SESSION.execute("SELECT * FROM simple_team WHERE teamName = ?", "A");
        List<Row> allRows = resultSet.all();
        assertThat(allRows).hasSize(2);
        assertThat(allRows.stream().map(r -> r.getInt("foundationYear"))).containsOnly(1993, 1994);
    }

    private void insertTeam(String name, int foundationYear, String country, int division) {
        Statement insert = QueryBuilder.insertInto("deleteTest", "simple_team")
                .value("teamName", name)
                .value("foundationYear", foundationYear)
                .value("country", country)
                .value("division", division)
                // Helps to make correct test on DELETE USING TIMESTAMP
                // Otherwise server-side timestamp is used and it not easy to predict
                // which value will be set.
                .setDefaultTimestamp(System.currentTimeMillis())
                .enableTracing();
        SESSION.execute(insert);
    }

}

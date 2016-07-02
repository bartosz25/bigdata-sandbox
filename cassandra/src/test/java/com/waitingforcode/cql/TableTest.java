package com.waitingforcode.cql;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.waitingforcode.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static com.waitingforcode.TestClient.*;
import static org.assertj.core.api.Assertions.assertThat;

public class TableTest {

    @BeforeClass
    public static void initContext() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS tableTest");
        SESSION.execute("CREATE KEYSPACE tableTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE tableTest");
        String teamPlayerQuery = TestHelper.readFromFile("/queries/create_static_team_player.cql");
        System.out.println("Executing query to create user type: "+teamPlayerQuery);
        SESSION.execute(teamPlayerQuery);
    }

    @AfterClass
    public static void destroy() {
        SESSION.execute("DROP KEYSPACE IF EXISTS tableTest");
    }

    @After
    public void cleanTeamPlayer() {
        SESSION.execute("TRUNCATE static_team_player");
    }

    @Test
    public void should_correctly_create_table_from_java_api() {
        Statement createStatement = SchemaBuilder.createTable("customer")
                .addPartitionKey("id", DataType.text())
                .addClusteringColumn("login", DataType.text())
                .addClusteringColumn("age", DataType.smallint())
                .withOptions().clusteringOrder("login", SchemaBuilder.Direction.DESC).comment("test comment").gcGraceSeconds(2);
        SESSION.execute(createStatement);

        KeyspaceMetadata ks = CLUSTER.getMetadata().getKeyspace("tableTest");
        TableMetadata table = ks.getTable("customer");

        assertThat(table).isNotNull();
        assertThat(table.getPartitionKey()).hasSize(1);
        assertThat(table.getPartitionKey().get(0).getName()).isEqualTo("id");
        assertThat(table.getClusteringColumns()).hasSize(2);
        assertThat(table.getClusteringColumns()).extracting("name").containsOnly("login", "age");
        assertThat(table.getOptions().getComment()).isEqualTo("test comment");
        assertThat(table.getOptions().getGcGraceInSeconds()).isEqualTo(2);
    }

    @Test
    public void should_correctly_work_with_static_column() {
        // To test if static column really work, first we create the rows
        // representing 1 players of a same team
        // After we INSERT new player under the same partition key but
        // with different value for static column with division
        // When we read the data after the change, modified column should
        // have new values for both rows (only 1 was explicitly edited)

        // However, the case doesn't work with update for this flow:
        // 1/ Create 2 players for the same partition key
        // 2/ Update static column for one of them
        // Expected error should be in this case:
        // Invalid restrictions on clustering columns since the UPDATE statement modifies only static columns
        String team = "RC Lens";
        int foundationYear = 1906;
        String country = "FR";
        int division = 2;
        SESSION.execute("INSERT INTO static_team_player (teamName, player, foundationYear, country, division) " +
                " VALUES (?, ?, ?, ?, ?)", team, "Player_1", foundationYear, country, division);


        ResultSet result = SESSION.execute("SELECT * FROM static_team_player");
        List<Row> rows = result.all();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString("teamName")).isEqualTo(team);
        assertThat(rows.get(0).getString("player")).isEqualTo("Player_1");
        assertThat(rows.get(0).getInt("foundationYear")).isEqualTo(foundationYear);
        assertThat(rows.get(0).getInt("division")).isEqualTo(2);

        // Now, add row with Player_2
        SESSION.execute("INSERT INTO static_team_player (teamName, player, foundationYear, country, division) " +
                " VALUES (?, ?, ?, ?, ?)", team, "Player_2", foundationYear, country, 1);

        result = SESSION.execute("SELECT * FROM static_team_player");
        rows = result.all();
        assertThat(rows).hasSize(2);
        assertThat(rows.stream().map(r -> r.getString("teamName"))).containsOnly(team);
        assertThat(rows.stream().map(r -> r.getString("player"))).containsOnly("Player_1", "Player_2");
        assertThat(rows.stream().map(r -> r.getInt("foundationYear"))).containsOnly(foundationYear);
        assertThat(rows.stream().map(r -> r.getInt("division"))).containsOnly(1);
    }

    @Test
    public void should_correctly_get_write_time_of_a_column() {
        long insertTime = System.currentTimeMillis();
        SESSION.execute("INSERT INTO static_team_player (teamName, player, foundationYear, country, division, bornYear) " +
                " VALUES ('Team1', 'Player1', 1999, 'BE', 1, 1980)");

        ResultSet writetimeResult = SESSION.execute("SELECT bornYear, WRITETIME(country) AS countryCreationTime FROM static_team_player " +
                " WHERE teamName = 'Team1' AND player = 'Player1'");

        assertThat(writetimeResult.one().getLong("countryCreationTime")).isEqualTo(insertTime*1000);
    }

}

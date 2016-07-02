package com.waitingforcode.primarykey;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.waitingforcode.TestHelper;
import com.waitingforcode.util.model.Player;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class ClusteringKeyTest {

    @BeforeClass
    public static void initContext() throws IOException, URISyntaxException {
        SESSION.execute("CREATE KEYSPACE IF NOT EXISTS football WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE football");
        String tableQuery = TestHelper.readFromFile("/queries/create_players.cql");
        System.out.println("Executing query to create table: "+tableQuery);
        SESSION.execute(tableQuery);
        String tableAscQuery = TestHelper.readFromFile("/queries/create_players_desc_order.cql");
        System.out.println("Executing query to create table: "+tableAscQuery);
        SESSION.execute(tableAscQuery);
    }

    @AfterClass
    public static void destroyContext() {
        SESSION.execute("DROP KEYSPACE IF EXISTS football");
    }

    @Test
    public void should_correctly_create_table_with_sorted_partition_keys_with_default_order() {
        // Default order for partition key is ascending
        SESSION.execute("USE football");
        SESSION.execute("INSERT INTO players_default (name, teamName) VALUES ('Luis Figo', 'Real Madrid')");
        SESSION.execute("INSERT INTO players_default (name, teamName) VALUES ('Raul', 'Real Madrid')");
        SESSION.execute("INSERT INTO players_default (name, teamName) VALUES ('David Beckham', 'Real Madrid')");
        SESSION.execute("INSERT INTO players_default (name, teamName) VALUES ('David Beckham', 'Manchester United')");
        SESSION.execute("INSERT INTO players_default (name, teamName) VALUES ('Raul', 'Schalke 04')");

        ResultSet resultSet = SESSION.execute("SELECT * FROM players_default WHERE teamName = 'Real Madrid'");

        List<Player> players = resultSet.all().stream().map(row -> Player.fromRow(row)).collect(Collectors.toList());
        assertThat(players).hasSize(3);
        assertThat(players).extracting("team").containsOnly("Real Madrid");
        assertThat(players).extracting("name").containsExactly("David Beckham", "Luis Figo", "Raul");
    }

    @Test
    public void should_correctly_create_table_with_sorted_asc_partition_keys() {
        SESSION.execute("USE football");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Luis Figo', 'Real Madrid')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Raul', 'Real Madrid')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('David Beckham', 'Real Madrid')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('David Beckham', 'New York')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Raul', 'Schalke 04')");


        ResultSet resultSet = SESSION.execute("SELECT * FROM players_descending WHERE teamName = 'Real Madrid'");

        List<Player> players = resultSet.all().stream().map(row -> Player.fromRow(row)).collect(Collectors.toList());
        assertThat(players).hasSize(3);
        assertThat(players).extracting("team").containsOnly("Real Madrid");
        assertThat(players).extracting("name").containsExactly("Raul", "Luis Figo", "David Beckham");
    }

    @Test(expected = InvalidQueryException.class)
    public void should_fail_on_explicitely_ordering_without_the_use_of_partition_key() {
        SESSION.execute("USE football");

        // Order clause can only be applied when partition key is restricted by "=" or "IN" clause. Otherwise,
        // following exception is thrown:
        // "InvalidQueryException: ORDER BY is only supported when the partition key is restricted by an EQ or an IN"
        // But as we can see in one of the next tests (XXXXXXXX), "IN" clause is not always welcomed when ordering..
        SESSION.execute("SELECT * FROM players_descending ORDER BY name ASC");
    }

    @Test(expected = InvalidQueryException.class)
    public void should_fail_on_explicit_ordering_with_in_clause() {
        SESSION.execute("USE football");
        // It's also illegal to use "IN" clause combined with ordering. This time, Cassandra returns
        // this error message:
        // "InvalidQueryException: Cannot page queries with both ORDER BY and a IN restriction on the partition key;
        // you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query"
        SESSION.execute("SELECT * FROM players_descending WHERE teamName IN('FC Barcelona', 'Schalke 04') ORDER BY name ASC");
    }

    @Test
    public void should_correctly_execute_query_with_in_clause_and_no_order() {
        SESSION.execute("USE football");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Jaap Stam', 'Ajax Amsterdam')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Javier Saviola', 'FC Barcelona')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Carles Puyol', 'FC Barcelona')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Andoni Zubizarreta', 'FC Barcelona')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Jaap Stam', 'Manchester United')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Laurent Blanc', 'Manchester United')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Fabien Barthez', 'Manchester United')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Patrick Kluivert', 'Ajax Amsterdam')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Kevin Kuranyi', 'Schalke 04')");
        SESSION.execute("INSERT INTO players_descending (name, teamName) VALUES ('Edwin van der Sar', 'Ajax Amsterdam')");

        // This time, when using IN clause without ordering, players are returned ordered in ascending way
        // by teams. The secondary order is made on player name, as indicated during table creation.
        // So, players should be orderd in descending way.
        ResultSet resultSet = SESSION.execute("SELECT * FROM players_descending WHERE team IN('FC Barcelona', " +
                "'Manchester United', 'Ajax Amsterdam')");

        List<Player> players = resultSet.all().stream().map(row -> Player.fromRow(row)).collect(Collectors.toList());
        assertThat(players).hasSize(9);
        assertThat(players).extracting("team").containsExactly("Ajax Amsterdam", "Ajax Amsterdam", "Ajax Amsterdam",
                "FC Barcelona", "FC Barcelona", "FC Barcelona", "Manchester United", "Manchester United", "Manchester United");
        assertThat(players).extracting("name").containsExactly("Patrick Kluivert", "Jaap Stam", "Edwin van der Sar",
                "Javier Saviola", "Carles Puyol", "Andoni Zubizarreta", "Laurent Blanc", "Jaap Stam", "Fabien Barthez");
    }

}

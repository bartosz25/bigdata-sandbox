package com.waitingforcode.consistency;


import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import com.waitingforcode.TestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.Assertions.*;

public class SimpleConsistencyTest {

    private static final Statement INSERT_TWO_CONSISTENCY = QueryBuilder.insertInto("consistencytest", "players_consistent")
            .value("name", "Player_1").value("teamName", "Team1")
            .setConsistencyLevel(ConsistencyLevel.TWO)
            .enableTracing();

    private static final Statement SELECT_TWO_CONSISTENCY = QueryBuilder.select().from("consistencytest", "players_consistent")
            .where(QueryBuilder.eq("name", "Fixed player")).setConsistencyLevel(ConsistencyLevel.TWO)
            .enableTracing();

    @BeforeClass
    public static void initContext() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS consistencytest");
        SESSION.execute("CREATE KEYSPACE consistencytest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}");
        SESSION.execute("USE consistencytest");
        String tableListQuery = TestHelper.readFromFile("/queries/consistency/create_players_consistency.cql");
        System.out.println("Executing query to create table: "+tableListQuery);
        SESSION.execute(tableListQuery);
        SESSION.execute("INSERT INTO players_consistent (name, teamName) VALUES ('Fixed player', 'Fixed team')");
    }

    @AfterClass
    public static void destroyContext() {
        SESSION.execute("DROP KEYSPACE IF EXISTS consistencytest");
    }


    @Test
    public void should_correctly_operate_on_data_with_TWO_consistency_level() {
        ResultSet result = SESSION.execute(INSERT_TWO_CONSISTENCY);

        assertThat(result.getExecutionInfo().getQueryTrace().getEvents()).extracting("name")
                .contains("Determining replicas for mutation", "Sending MUTATION message", "MUTATION message received");
    }

    @Test
    public void should_fail_on_inserting_data_with_consistency_TWO_when_one_node_is_down() throws InterruptedException {
        System.out.println("Before launching the test, please stop one of 2 running nodes...");
        Thread.sleep(10000);

        try {
            SESSION.execute(INSERT_TWO_CONSISTENCY);
            fail("Should not execute INSERT with consistency TWO when only one replica is running");
        } catch (NoHostAvailableException exception) {
            assertThat(exception.getMessage()).contains("Not enough replicas available for query at consistency TWO");
        }
    }

    @Test
    public void should_correctly_read_data_with_TWO_consistency_level_when_2_nodes_are_running() {
        ResultSet result = SESSION.execute(SELECT_TWO_CONSISTENCY);


        List<String> expected = Lists.newArrayList("Merging data from memtables and 0 sstables", "Acquiring sstable references");
        List<QueryTrace.Event> events = result.getExecutionInfo().getQueryTrace().getEvents().stream()
                .filter(event -> expected.contains(event.getDescription())).collect(Collectors.toList());
        assertThat(events).hasSize(4);
        assertThat(events.stream().map(e -> e.getSource().toString())).containsOnly("/127.0.0.1", "/127.0.0.2");
    }

    @Test
    public void should_fail_read_when_only_1_node_is_running_while_TWO_consistency_is_set() throws InterruptedException {
        System.out.println("Before launching the test, please stop one of 2 running nodes...");
        Thread.sleep(10000);

        try {
            SESSION.execute(SELECT_TWO_CONSISTENCY);
            fail("Should not execute SELECT with consistency TWO when only one replica is running");
        }  catch (NoHostAvailableException exception) {
            assertThat(exception.getMessage()).contains("Not enough replicas available for query at consistency TWO");
        }
    }

}

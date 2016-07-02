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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class QuorumConsistencyTest {

    private static final Statement INSERT_QUORUM_CONSISTENCY = QueryBuilder.insertInto("quorumconsistencytest", "players_consistent")
            .value("name", "Player_1").value("teamName", "Team1")
            .setConsistencyLevel(ConsistencyLevel.QUORUM)
            .enableTracing();

    private static final Statement SELECT_QUORUM_CONSISTENCY = QueryBuilder.select().from("quorumconsistencytest", "players_consistent")
            .where(QueryBuilder.eq("name", "Fixed player")).setConsistencyLevel(ConsistencyLevel.QUORUM)
            .enableTracing();

    @BeforeClass
    public static void initContext() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS quorumconsistencytest");
        SESSION.execute("CREATE KEYSPACE quorumconsistencytest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2}");
        SESSION.execute("USE quorumconsistencytest");
        String tableListQuery = TestHelper.readFromFile("/queries/consistency/create_players_consistency.cql");
        System.out.println("Executing query to create table: "+tableListQuery);
        SESSION.execute(tableListQuery);
        SESSION.execute("INSERT INTO players_consistent (name, teamName) VALUES ('Fixed player', 'Fixed team')");
    }

    @AfterClass
    public static void destroyContext() {
        SESSION.execute("DROP KEYSPACE IF EXISTS quorumconsistencytest");
    }

    @Test
    public void should_correctly_insert_data_when_all_nodes_are_up() {
        ResultSet result = SESSION.execute(INSERT_QUORUM_CONSISTENCY);

        assertThat(result.getExecutionInfo().getQueryTrace().getEvents()).extracting("name")
                .contains("Determining replicas for mutation",
                        "Sending MUTATION message to /127.0.0.2", "Sending MUTATION message to /127.0.0.1");
    }

    @Test
    public void should_correctly_read_data_when_accepted_1_node_is_down_in_quorum() throws InterruptedException {
        System.out.println("Before launching the test, please stop one of 3 running nodes...");
        Thread.sleep(10000);

        ResultSet result = SESSION.execute(SELECT_QUORUM_CONSISTENCY);

        List<String> expected = Lists.newArrayList("Merging data from memtables and 0 sstables", "Acquiring sstable references");
        List<QueryTrace.Event> events = result.getExecutionInfo().getQueryTrace().getEvents().stream()
                .filter(event -> expected.contains(event.getDescription())).collect(Collectors.toList());
        assertThat(events).hasSize(4);
        assertThat(events.stream().map(e -> e.getSource().toString())).containsOnly("/127.0.0.1", "/127.0.0.2");
    }

    @Test
    public void should_fail_on_reading_data_when_2_nodes_are_down_in_quorum() throws InterruptedException {
        System.out.println("Before launching the test, please stop 2 of 3 running nodes...");
        Thread.sleep(10000);

        try {
            SESSION.execute(SELECT_QUORUM_CONSISTENCY);
            fail("Should not be able to SELECT data when QUORUM consistency is not respected");
        } catch (NoHostAvailableException e) {
           assertThat(e.getMessage()).contains(
                   "Not enough replicas available for query at consistency QUORUM (2 required but only 1 alive)");
        }
    }

    @Test
    public void should_fail_on_inserting_data_when_2_nodes_are_down_in_quorum() throws InterruptedException {
        System.out.println("Before launching the test, please stop 2 of 3 running nodes...");
        Thread.sleep(10000);

        try {
            SESSION.execute(INSERT_QUORUM_CONSISTENCY);
            fail("Should not be able to INSERT data when QUORUM consistency is not respected");
        } catch (NoHostAvailableException e) {
            assertThat(e.getMessage()).contains(
                    "Not enough replicas available for query at consistency QUORUM (2 required but only 1 alive)");
        }
    }

}

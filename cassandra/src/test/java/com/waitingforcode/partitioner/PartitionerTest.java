package com.waitingforcode.partitioner;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.MoreObjects;
import com.waitingforcode.TestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * To see partitioning variations, change partitioner entry in cassandra.yaml file
 * before launching test cases.
 */
public class PartitionerTest {

    private static final String[] TEAMS = {"Ajax Amsterdam", "ADO Den Hag", "Atletico Madrid", "AC Ajaccio", "Athletic Bilbao",
            "Dynamo Kiev", "Dynamo Lviev", "Dnipro", "Lille OSC", "Zamalek"};
    private static final Collection<PartitionStats> INSERT_STATS = new ArrayList<>();

    @BeforeClass
    public static void prepareContext() throws IOException, URISyntaxException, InterruptedException {
        SESSION.execute("DROP KEYSPACE IF EXISTS partitionertest");
        SESSION.execute("CREATE KEYSPACE partitionertest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE partitionertest");
        String tableListQuery = TestHelper.readFromFile("/queries/create_simple_team.cql");
        System.out.println("Executing query to create table: "+tableListQuery);
        SESSION.execute(tableListQuery);
        Thread.sleep(5000);
        for (String team : TEAMS) {
            ResultSet result = insertTeam(team);
            INSERT_STATS.add(new PartitionStats(team, result.getExecutionInfo().getQueryTrace().getCoordinator(),
                            getNodeHoldingData(result.getExecutionInfo())));
        }
    }

    @AfterClass
    public static void destroyContext() {
        SESSION.execute("DROP KEYSPACE IF EXISTS partitionertest");
    }

    @Test
    public void should_read_rows_from_the_same_node_as_during_insert() throws IOException, URISyntaxException, InterruptedException {
        for (String team : TEAMS) {
            Statement selectAllStm = QueryBuilder.select().from("partitionertest", "simple_team")
                    .where(QueryBuilder.eq("teamName", team))
                    .enableTracing();
            ResultSet result = SESSION.execute(selectAllStm);

            PartitionStats selectStats = new PartitionStats(team, result.getExecutionInfo().getQueryTrace().getCoordinator(),
                    getNodeHoldingData(result.getExecutionInfo()));
            PartitionStats insertStats = findStatsByTeam(team);

            assertThat(insertStats.team).isEqualTo(selectStats.team);
            assertThat(insertStats.data).isEqualTo(selectStats.data);
        }

        System.out.println("> "+INSERT_STATS);
    }

    private static InetAddress getNodeHoldingData(ExecutionInfo executionInfo) {
        InetAddress dataNodeAddress = executionInfo.getQueryTrace().getCoordinator();
        // Try to check if list contains other address - if yes, it's the address of node with data
        Optional<QueryTrace.Event> otherNode = executionInfo.getQueryTrace().getEvents().stream()
                .filter(e -> !e.getSource().equals(dataNodeAddress))
                .findFirst();

        return otherNode.isPresent() ? otherNode.get().getSource() : dataNodeAddress;
    }

    private PartitionStats findStatsByTeam(String team) {
        return INSERT_STATS.stream().filter(s -> s.team.equals(team)).findFirst().get();
    }

    private static ResultSet insertTeam(String teamName) {
        Statement insert = QueryBuilder.insertInto("partitionertest", "simple_team")
                .value("teamName", teamName)
                .enableTracing();
        return SESSION.execute(insert);
    }

    private static class PartitionStats {
        private final String team;
        private final InetAddress coordinator;
        private final InetAddress data;

        private PartitionStats(String team, InetAddress coordinator, InetAddress data) {
            this.team = team;
            this.coordinator = coordinator;
            this.data = data;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("team", team).add("coordinator node", coordinator)
                    .add("data node", data).toString();
        }
    }
}

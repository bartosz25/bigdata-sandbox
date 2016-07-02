package com.waitingforcode.cache;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.waitingforcode.TestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class CounterCacheTest {

    @BeforeClass
    public static void initContext() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS countercachetest");
        SESSION.execute("CREATE KEYSPACE countercachetest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE countercachetest");
        String teamPlayerQuery = TestHelper.readFromFile("/queries/create_players_counter.cql");
        System.out.println("Executing query to create user type: "+teamPlayerQuery);
        SESSION.execute(teamPlayerQuery);
    }

    @AfterClass
    public static void destroy() {
        SESSION.execute("DROP KEYSPACE IF EXISTS countercachetest");
    }

    @Test
    public void should_use_counter_cache_when_working_with_counter_column() {
        for (int i = 0; i < 10; i++) {
            Statement queryUpdate = QueryBuilder.update("countercachetest", "players_default_counter")
                    .with(incr("games")).where(eq("name", "Player")).and(eq("teamName", "Team"))
                    .enableTracing();
            ResultSet resultSet = SESSION.execute(queryUpdate);
            ExecutionInfo info = resultSet.getExecutionInfo();
            // As you can see, when counter cache is enabled, execution traces
            // should return similar entries to this one:
            assertThat(info.getQueryTrace().getEvents()).extracting("name").contains("Fetching 1 counter values from cache");
        }
    }

}

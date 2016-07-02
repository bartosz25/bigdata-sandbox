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

import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class RowCacheTest {

    // Before launching, check if row_cache_size_in_mb
    // is enabled in the configuration
    @BeforeClass
    public static void initContext() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS rowcachetest");
        SESSION.execute("CREATE KEYSPACE rowcachetest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE rowcachetest");
        String teamPlayerQuery = TestHelper.readFromFile("/queries/create_players_cache.cql");
        System.out.println("Executing query to create user type: "+teamPlayerQuery);
        SESSION.execute(teamPlayerQuery);
    }

    @AfterClass
    public static void destroy() {
        SESSION.execute("DROP KEYSPACE IF EXISTS rowcachetest");
    }

    @Test
    public void should_read_data_from_row_cache() {
        for (int i = 0; i < 10; i++) {
            Statement queryInsert = QueryBuilder.insertInto("rowcachetest", "players_default_cached")
                    .value("name", "Player").value("teamName", "Team_"+i);
            SESSION.execute(queryInsert);
        }

        // Shouldn't use row cache
        Statement read1Query = QueryBuilder.select().from("rowcachetest", "players_default_cached")
                .where(QueryBuilder.eq("name", "Player")).enableTracing();
        ResultSet resultSet = SESSION.execute(read1Query);
        ExecutionInfo info = resultSet.getExecutionInfo();
        assertThat(info.getQueryTrace().getEvents()).extracting("name").contains("Row cache miss")
                .doesNotContain("Row cache hit");
        int noCacheEventsLength = info.getQueryTrace().getEvents().size();
        info.getQueryTrace().getEvents().forEach(event -> System.out.println("Event without cache: "+event));

        // After the first lookup, the row should be put inside cache
        resultSet = SESSION.execute(read1Query);
        info = resultSet.getExecutionInfo();
        assertThat(info.getQueryTrace().getEvents()).extracting("name").contains("Row cache hit")
                .doesNotContain("Row cache miss");
        assertThat(info.getQueryTrace().getEvents().size()).isLessThan(noCacheEventsLength);
        info.getQueryTrace().getEvents().forEach(event -> System.out.println("Event with cache: "+event));
    }

    @Test
    public void should_read_data_from_row_cache_only_once_because_of_data_change() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            Statement queryInsert = QueryBuilder.insertInto("rowcachetest", "players_default_cached")
                    .value("name", "Player2").value("teamName", "Team_"+i);
            SESSION.execute(queryInsert);
        }

        // Shouldn't use row cache
        Statement read1Query = QueryBuilder.select().from("rowcachetest", "players_default_cached")
                .where(QueryBuilder.eq("name", "Player2")).enableTracing();
        ResultSet resultSet = SESSION.execute(read1Query);
        ExecutionInfo info = resultSet.getExecutionInfo();
        assertThat(info.getQueryTrace().getEvents()).extracting("name").contains("Row cache miss")
                .doesNotContain("Row cache hit");
        int noCacheEventsLength = info.getQueryTrace().getEvents().size();
        info.getQueryTrace().getEvents().forEach(event -> System.out.println("Event without cache: "+event));

        // After the first lookup, the row should be put inside cache
        resultSet = SESSION.execute(read1Query);
        info = resultSet.getExecutionInfo();
        assertThat(info.getQueryTrace().getEvents()).extracting("name").contains("Row cache hit")
                .doesNotContain("Row cache miss");
        assertThat(info.getQueryTrace().getEvents().size()).isLessThan(noCacheEventsLength);
        info.getQueryTrace().getEvents().forEach(event -> System.out.println("Event with cache: "+event));

        // Now, add some data and check if read is still made from cache
        // It shouldn't
        Statement queryInsert = QueryBuilder.insertInto("rowcachetest", "players_default_cached")
                .value("name", "Player2").value("teamName", "Team_Bis");
        SESSION.execute(queryInsert);
        resultSet = SESSION.execute(read1Query);
        info = resultSet.getExecutionInfo();
        assertThat(info.getQueryTrace().getEvents()).extracting("name").contains("Row cache miss")
                .doesNotContain("Row cache hit");
    }

}

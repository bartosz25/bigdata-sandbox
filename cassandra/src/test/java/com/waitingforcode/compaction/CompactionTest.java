package com.waitingforcode.compaction;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.waitingforcode.TestHelper;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class CompactionTest {

    // TODO : testowac mozna to poprzez flush danych z pamieci, co spowoduje powstanie SSTables

    @BeforeClass
    public static void initDataset() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS compactiontest");
        SESSION.execute("CREATE KEYSPACE compactiontest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE compactiontest");
        String sizeCompactionQuery = TestHelper.readFromFile("/queries/compaction/create_team_size_compaction.cql");
        System.out.println("Executing query to create table: "+sizeCompactionQuery);
        SESSION.execute(sizeCompactionQuery);
    }

    //@AfterClass
    public static void destroy() {
        SESSION.execute("DROP KEYSPACE IF EXISTS compactiontest");
    }

    @Test
    public void should_read_teams_in_the_order_of_data_adding() throws InterruptedException {
        for (int i = 0; i < 500_000; i++) {
            Statement insert = QueryBuilder.insertInto("compactiontest", "simple_team")
                    .value("city", "old")
                    .value("teamName", "Team_"+i);
            SESSION.execute(insert);
        }

        for (int i = 0; i < 500_000; i++) {
            Statement update = QueryBuilder.update("compactiontest", "simple_team")
                    .where(QueryBuilder.eq("teamName", "Team_"+i))
                    .with(QueryBuilder.set("city", "new"));
            SESSION.execute(update);
        }

        for (int i= 0; i < 500_000; i++) {
            Statement select = QueryBuilder.select()
                    .from("compactiontest", "simple_team")
                    .where(QueryBuilder.eq("teamName", "Team_"+i));
            SESSION.execute(select);
        }

        Thread.sleep(5000);

        boolean wasFound = false;
        ResultSet resultSet = SESSION.execute("SELECT * FROM system.compaction_history");
        for (Row row : resultSet) {
            if (row.getString("keyspace_name").equals("compactiontest")
                    && row.getString("columnfamily_name").equals("simple_team")) {
                wasFound = true;
                break;
            }
        }

        assertThat(wasFound).isTrue();
    }

}

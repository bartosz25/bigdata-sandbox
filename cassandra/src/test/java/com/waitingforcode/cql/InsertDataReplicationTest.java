package com.waitingforcode.cql;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.waitingforcode.TestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.waitingforcode.TestClient.SESSION;

public class InsertDataReplicationTest {

    @BeforeClass
    public static void initKeyspace() {
        SESSION.execute("CREATE KEYSPACE IF NOT EXISTS players WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("CREATE KEYSPACE IF NOT EXISTS network_top_bad_dc WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " +
                "'data_center_1' : 1}");
        SESSION.execute("CREATE KEYSPACE IF NOT EXISTS network_top_good_dc WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " +
                "'"+ TestClient.getDatacenterName()+"' : 1}");
    }

    @AfterClass
    public static void deleteKeyspaces() {
        TestClient.SESSION.execute("DROP KEYSPACE IF EXISTS players");
        TestClient.SESSION.execute("DROP KEYSPACE IF EXISTS network_top_bad_dc");
        TestClient.SESSION.execute("DROP KEYSPACE IF EXISTS network_top_good_dc");
    }

    @Test(expected = NoHostAvailableException.class)
    public void should_fail_inserting_data_when_node_does_not_belong_to_keyspace_data_center() {
        // Here we try to add a row to table needing to be replicated in 'data_center_1' once.
        // Since this data center doesn't exist, this try should fail with an error telling that:
        // Not enough replicas available for query at consistency LOCAL_ONE (1 required but only 0 alive)))
        SESSION.execute("USE network_top_bad_dc");
        SESSION.execute("CREATE TABLE test (age int PRIMARY KEY)");
        SESSION.execute("INSERT INTO test (age) VALUES (10)");
    }

    @Test
    public void should_correctly_insert_data_on_node_belonging_to_keyspace_data_center() {
        // This time we add a row to keyspace under network replication topology in existing
        // data center. It should not cause any problems.
        SESSION.execute("USE network_top_good_dc");
        SESSION.execute("CREATE TABLE test (age int PRIMARY KEY)");
        SESSION.execute("INSERT INTO test (age) VALUES (10)");
    }

}

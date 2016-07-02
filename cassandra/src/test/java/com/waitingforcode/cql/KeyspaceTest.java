package com.waitingforcode.cql;

import com.datastax.driver.core.ResultSet;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.waitingforcode.TestClient.*;
import static org.assertj.core.api.Assertions.assertThat;

public class KeyspaceTest {

    @BeforeClass
    public static void cleanTables() {
        /*
        Corresponding logs entry:
        Drop Keyspace 'players'
         */
        SESSION.execute("DROP KEYSPACE IF EXISTS players");
    }

    @Test
    public void should_correctly_create_keyspace() {
        /*
            Corresponding logs entry:
            "Create new Keyspace: KeyspaceMetadata{name=players, params=KeyspaceParams{durable_writes=true,
            replication=ReplicationParams{class=org.apache.cassandra.locator.SimpleStrategy,
            replication_factor=0}}, tables=[], views=[], functions=[], types=[]}

         */

        ResultSet result =
                SESSION.execute("CREATE KEYSPACE players WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 0}");

        assertThat(result.wasApplied()).isTrue();
        assertThat(result.isExhausted()).isTrue();
    }

    @Test
    public void sshould_correctly_create_keyspace() {
        /*
            Corresponding logs entry:
            "Create new Keyspace: KeyspaceMetadata{name=players, params=KeyspaceParams{durable_writes=true,
            replication=ReplicationParams{class=org.apache.cassandra.locator.SimpleStrategy,
            replication_factor=0}}, tables=[], views=[], functions=[], types=[]}

         */

        ResultSet result =
                SESSION.execute("CREATE KEYSPACE players WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3}");

        assertThat(result.wasApplied()).isTrue();
        assertThat(result.isExhausted()).isTrue();
    }


    @Test
    public void should_correctly_create_keyspace_with_network_topology_when_cassandra_is_ran_in_standalone() {
        // Even if we haven't defined multiple data centers yet, it's possible to create correctly
        // a keyspace configured to be replicated through multiple data centers.
        // However, the problem will come further, when we'll try to insert data.
        ResultSet result =
                SESSION.execute("CREATE KEYSPACE players WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " +
                        "'rack1' : 3, 'rack2': 10}");

        assertThat(result.wasApplied()).isTrue();
        assertThat(result.isExhausted()).isTrue();
    }


}

package com.waitingforcode;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;

public final class TestClient {

    public static final Cluster CLUSTER = Cluster.builder().addContactPoint("127.0.0.1").build();
    public static final Session SESSION = CLUSTER.connect();
    public static final MappingManager MAPPING_MANAGER = new MappingManager(SESSION);

    private TestClient() {
        // prevents init
    }

    public static String getDatacenterName() {
        SESSION.execute("USE system");
        ResultSet rows = SESSION.execute("SELECT data_center FROM local");
        Row row = rows.one();
        return row.get("data_center", String.class);
    }

}

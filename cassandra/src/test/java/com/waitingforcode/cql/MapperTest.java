package com.waitingforcode.cql;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.mapping.Mapper;
import com.google.common.util.concurrent.ListenableFuture;
import com.waitingforcode.TestHelper;
import com.waitingforcode.util.model.SimpleTeam;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.waitingforcode.TestClient.*;
import static org.assertj.core.api.Assertions.assertThat;

public class MapperTest {
    @BeforeClass
    public static void initDataset() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS mapperTest");
        SESSION.execute("CREATE KEYSPACE mapperTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE mapperTest");
    }

    @AfterClass
    public static void destroy() {
        SESSION.execute("DROP KEYSPACE IF EXISTS mapperTest");
    }

    @Before
    public void deleteTable() {
        SESSION.execute("DROP TABLE IF EXISTS mapperTest.simple_team");
    }

    @Test(expected = InvalidQueryException.class)
    public void should_fail_on_inserting_row_without_defined_table() {
        SESSION.execute("DROP TABLE IF EXISTS mapperTest.simple_team");
        // Please note that the table doesn't exist - was not created as previously in setup method
        // It makes this test fail because tables aren't created in the fly
        Mapper<SimpleTeam> mapperTeam = MAPPING_MANAGER.mapper(SimpleTeam.class);
        mapperTeam.save(new SimpleTeam("AC Ajaccio", 1910, "France", 3));
    }

    @Test
    public void should_create_only_one_mapper_instance() throws IOException, URISyntaxException {
        createMappedTable();

        Mapper<SimpleTeam> mapperTeam1 = MAPPING_MANAGER.mapper(SimpleTeam.class);
        Mapper<SimpleTeam> mapperTeam2 = MAPPING_MANAGER.mapper(SimpleTeam.class);

        assertThat(mapperTeam1).isEqualTo(mapperTeam2);
    }

    @Test
    public void should_correctly_manipulate_row_through_mapper() throws IOException, URISyntaxException {
        createMappedTable();

        Mapper<SimpleTeam> mapperTeam = MAPPING_MANAGER.mapper(SimpleTeam.class);
        mapperTeam.save(new SimpleTeam("AC Ajaccio", 1910, "France", 3));

        SimpleTeam acAjaccio = mapperTeam.get("AC Ajaccio", 1910, "France");
        assertThat(acAjaccio).isNotNull();
        assertThat(acAjaccio.getCountry()).isEqualTo("France");
        assertThat(acAjaccio.getFoundationYear()).isEqualTo(1910);
        assertThat(acAjaccio.getTeamName()).isEqualTo("AC Ajaccio");
        assertThat(acAjaccio.getDivision()).isEqualTo(3);
    }

    @Test
    public void should_correctly_delete_created_team() throws IOException, URISyntaxException {
        createMappedTable();

        Mapper<SimpleTeam> mapperTeam = MAPPING_MANAGER.mapper(SimpleTeam.class);
        mapperTeam.save(new SimpleTeam("AC Ajaccio", 1910, "France", 2));

        SimpleTeam acAjaccio = mapperTeam.get("AC Ajaccio", 1910, "France");
        assertThat(acAjaccio).isNotNull();

        mapperTeam.delete(acAjaccio);
        acAjaccio = mapperTeam.get("AC Ajaccio", 1910, "France");
        assertThat(acAjaccio).isNull();
    }

    @Test
    public void should_edit_team_in_asynchronous_way() throws IOException, URISyntaxException, InterruptedException, ExecutionException, TimeoutException {
        // Non-blocking operations are provided with mapper module
        // One of them is edition
        createMappedTable();

        Mapper<SimpleTeam> mapperTeam = MAPPING_MANAGER.mapper(SimpleTeam.class);
        mapperTeam.save(new SimpleTeam("AC Ajaccio", 1910, "France", 2));

        SimpleTeam acAjaccio = mapperTeam.get("AC Ajaccio", 1910, "France");
        assertThat(acAjaccio).isNotNull();

        acAjaccio.setDivision(1);
        ListenableFuture<Void> future = mapperTeam.saveAsync(acAjaccio);
        future.get(5, TimeUnit.SECONDS);

        SimpleTeam acAjaccioDiv1= mapperTeam.get("AC Ajaccio", 1910, "France");
        assertThat(acAjaccioDiv1).isNotNull();
        assertThat(acAjaccioDiv1.getDivision()).isEqualTo(1);
    }

    @Test
    public void should_not_edit_row_when_one_primary_key_changes() throws IOException, URISyntaxException {
        // When one of primary keys changes, it means that new row is created
        // Cassandra doesn't allow to update PRIMARY KEYs
        createMappedTable();

        Mapper<SimpleTeam> mapperTeam = MAPPING_MANAGER.mapper(SimpleTeam.class);
        mapperTeam.save(new SimpleTeam("AC Ajaccio", 1910, "France", 2));

        SimpleTeam acAjaccio = mapperTeam.get("AC Ajaccio", 1910, "France");
        assertThat(acAjaccio).isNotNull();
        acAjaccio.setCountry("Corsica");
        mapperTeam.save(acAjaccio);

        SimpleTeam acAjaccioFrance= mapperTeam.get("AC Ajaccio", 1910, "France");
        SimpleTeam acAjaccioCorsica= mapperTeam.get("AC Ajaccio", 1910, "Corsica");
        assertThat(acAjaccioFrance).isNotNull();
        assertThat(acAjaccioFrance.getCountry()).isEqualTo("France");
        assertThat(acAjaccioCorsica).isNotNull();
        assertThat(acAjaccioCorsica.getCountry()).isEqualTo("Corsica");
    }

    private void createMappedTable() throws IOException, URISyntaxException {
        String tableCreateQuery = TestHelper.readFromFile("/queries/create_simple_team.cql");
        System.out.println("Executing query to create table: "+tableCreateQuery);
        SESSION.execute(tableCreateQuery);
    }

}

package com.waitingforcode.serialization;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ClubSerializationTest {

    private static Schema CLUB_SCHEMA_WRITE;
    private static final String RC_LENS_AVRO = "rc_lens.avro";
    private static final String MATRA_AVRO = "matra_racing.avro";

    @BeforeClass
    public static void initClubSchema() throws IOException {
        CLUB_SCHEMA_WRITE = new Schema.Parser().parse(
            new File(ClubSerializationTest.class.getClassLoader().getResource("schemas/club.json").getFile())
        );
    }

    @Test
    public void should_correctly_serialize_disappeared_club() throws IOException {
        GenericRecord rcLens = new GenericData.Record(CLUB_SCHEMA_WRITE);
        rcLens.put("full_name", "RC Lens");
        rcLens.put("foundation_year", 1906);
        // null value doesn't need to be specified

        // Serialize first and deserialize after
        File outputClubsFile = File.createTempFile(RC_LENS_AVRO, "");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(CLUB_SCHEMA_WRITE);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, outputClubsFile);
        dataFileWriter.append(rcLens);
        dataFileWriter.close();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(CLUB_SCHEMA_WRITE);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isTrue();
        assertThat(dataFileReader.next(null).toString())
                .isEqualTo("{\"full_name\": \"RC Lens\", \"foundation_year\": 1906, \"disappearance_year\": null}");
   }

    @Test
    public void should_fail_on_flushing_serialization_without_close_call() throws IOException {
        GenericRecord racingMatra = new GenericData.Record(CLUB_SCHEMA_WRITE);
        racingMatra.put("full_name", "Matra Racing");
        racingMatra.put("foundation_year", 1987);
        racingMatra.put("disappearance_year", 1989);

        File outputClubsFile = File.createTempFile(MATRA_AVRO, "");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(CLUB_SCHEMA_WRITE);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, outputClubsFile);
        dataFileWriter.append(racingMatra);
        // no flush() call
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(CLUB_SCHEMA_WRITE);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isFalse();
        dataFileWriter.close();
    }

    @Test
    public void should_correctly_serialize_club_with_flush() throws IOException {
        GenericRecord racingMatra = new GenericData.Record(CLUB_SCHEMA_WRITE);
        racingMatra.put("full_name", "Matra Racing");
        racingMatra.put("foundation_year", 1987);
        racingMatra.put("disappearance_year", 1989);

        File outputClubsFile = File.createTempFile(MATRA_AVRO, "");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(CLUB_SCHEMA_WRITE);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, outputClubsFile);
        dataFileWriter.append(racingMatra);
        dataFileWriter.flush();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(CLUB_SCHEMA_WRITE);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isTrue();
        dataFileWriter.close();
    }

    @Test
    public void should_detect_that_json_with_one_entry_is_smaller_than_avro_with_one_item() throws IOException {
        File jsonFile =
                new File(ClubSerializationTest.class.getClassLoader().getResource("output/single_rc_lens.json").getFile());

        GenericRecord rcLens = new GenericData.Record(CLUB_SCHEMA_WRITE);
        rcLens.put("full_name", "RC Lens");
        rcLens.put("foundation_year", 1906);

        // Serialize first and deserialize after
        File outputClubsFile = File.createTempFile(RC_LENS_AVRO, "");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(CLUB_SCHEMA_WRITE);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, outputClubsFile);
        dataFileWriter.append(rcLens);
        dataFileWriter.close();

        System.out.println("Avro file length="+outputClubsFile.length());
        System.out.println("JSON file length="+jsonFile.length());
        // Because Avro stores writer's mapping, it's logically bigger than
        // plain JSON file storing only key-value pairs
        // Expected Avro size is 359 while JSON only 77 bytes
        assertThat(outputClubsFile.length()).isGreaterThan(jsonFile.length());
    }

    @Test
    public void should_detect_that_json_with_6_lines_is_bigger_than_avro_with_the_same_numer_of_items() throws IOException {
        // Since Avro doesn't repeat field names in saved file, from specific point of serialized items,
        // it occupies less place than, for example, original JSON files
        File jsonFile =
                new File(ClubSerializationTest.class.getClassLoader().getResource("output/6_teams.json").getFile());

        // Serialize first and deserialize after
        File outputClubsFile = File.createTempFile(RC_LENS_AVRO, "");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(CLUB_SCHEMA_WRITE);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, outputClubsFile);
        createTeams(6).forEach(team -> append(dataFileWriter, team));
        dataFileWriter.close();

        System.out.println("Avro file length="+outputClubsFile.length());
        System.out.println("JSON file length="+jsonFile.length());
        // The difference should be quite important, of order of 11 bytes; 437 for Avro, 494 for JSON
        assertThat(jsonFile.length()).isGreaterThan(outputClubsFile.length());
    }

    @Test
    public void should_detect_that_5_lines_in_json_is_smaller_than_in_avro() throws IOException {
        File jsonFile =
                new File(ClubSerializationTest.class.getClassLoader().getResource("output/5_teams.json").getFile());
        // Serialize first and deserialize after
        File outputClubsFile = File.createTempFile(RC_LENS_AVRO, "");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(CLUB_SCHEMA_WRITE);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, outputClubsFile);
        createTeams(5).forEach(team -> append(dataFileWriter, team));
        dataFileWriter.close();

        System.out.println("Avro file length="+outputClubsFile.length());
        System.out.println("JSON file length="+jsonFile.length());
        // The difference should be small, of order of 11 bytes; 422 for Avro, 411 for JSON
        assertThat(outputClubsFile.length()).isGreaterThan(jsonFile.length());
    }

    private void append(DataFileWriter<GenericRecord> dataFileWriter, GenericRecord record) {
        try {
            dataFileWriter.append(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<GenericRecord> createTeams(int number) {
        List<GenericRecord> teams = new ArrayList<>(number);
        GenericRecord rcLens = new GenericData.Record(CLUB_SCHEMA_WRITE);
        rcLens.put("full_name", "RC Lens");
        rcLens.put("foundation_year", 1906);
        teams.add(rcLens);
        GenericRecord lilleOsc = new GenericData.Record(CLUB_SCHEMA_WRITE);
        lilleOsc.put("full_name", "Lille OSC");
        lilleOsc.put("foundation_year", 1944);
        teams.add(lilleOsc);
        GenericRecord matraRacig = new GenericData.Record(CLUB_SCHEMA_WRITE);
        matraRacig.put("full_name", "Matra Racing");
        matraRacig.put("foundation_year", 1987);
        matraRacig.put("disappearance_year", 1989);
        teams.add(matraRacig);
        GenericRecord usValenciennes = new GenericData.Record(CLUB_SCHEMA_WRITE);
        usValenciennes.put("full_name", "US Valenciennes");
        usValenciennes.put("foundation_year", 1913);
        teams.add(usValenciennes);
        GenericRecord parisSg = new GenericData.Record(CLUB_SCHEMA_WRITE);
        parisSg.put("full_name", "Paris-SG");
        parisSg.put("foundation_year", 1970);
        teams.add(parisSg);

        if (number == 6) {
            GenericRecord redStart = new GenericData.Record(CLUB_SCHEMA_WRITE);
            redStart.put("full_name", "Red Star 93");
            redStart.put("foundation_year", 1897);
            teams.add(redStart);
        }
        return teams;
    }

}

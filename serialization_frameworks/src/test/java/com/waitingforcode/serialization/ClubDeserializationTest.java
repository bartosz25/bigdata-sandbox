package com.waitingforcode.serialization;

import com.waitingforcode.model.Club;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.util.Utf8;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class ClubDeserializationTest {

    private static Schema CLUB_SCHEMA_WRITE;
    private static Schema CLUB_SCHEMA_READ;
    private static Schema CLUB_BAD_SCHEMA_READ;
    private static final String RC_LENS_AVRO = "rc_lens.avro";
    private static final String TWO_TEAMS_AVRO = "2_teams.avro";

    @BeforeClass
    public static void setupContext() throws IOException {
        CLUB_SCHEMA_WRITE = new Schema.Parser().parse(
                new File(ClubSerializationTest.class.getClassLoader().getResource("schemas/club.json").getFile())
        );
        CLUB_SCHEMA_READ = new Schema.Parser().parse(
                new File(ClubSerializationTest.class.getClassLoader().getResource("schemas/club_read.json").getFile())
        );
        CLUB_BAD_SCHEMA_READ = new Schema.Parser().parse(
                new File(ClubSerializationTest.class.getClassLoader().getResource("schemas/club_bad_package.json").getFile())
        );
        GenericRecord rcLens = new GenericData.Record(CLUB_SCHEMA_WRITE);
        rcLens.put("full_name", "RC Lens");
        rcLens.put("foundation_year", 1906);
        GenericRecord lilleOsc = new GenericData.Record(CLUB_SCHEMA_WRITE);
        lilleOsc.put("full_name", "Lille OSC");
        lilleOsc.put("foundation_year", 1944);

        File rcLensFile = new File(RC_LENS_AVRO);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(CLUB_SCHEMA_WRITE);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, rcLensFile);
        dataFileWriter.append(rcLens);
        dataFileWriter.close();

        File twoTeamsFile = new File(TWO_TEAMS_AVRO);
        dataFileWriter.create(CLUB_SCHEMA_WRITE, twoTeamsFile);
        dataFileWriter.append(rcLens);
        dataFileWriter.append(lilleOsc);
        dataFileWriter.close();

        rcLensFile.deleteOnExit();
        twoTeamsFile.deleteOnExit();
    }

    @Test
    public void should_correctly_deserialize_club_with_reader_schema() throws IOException {
        File outputClubsFile = new File(RC_LENS_AVRO);

        DatumReader<Club> datumReader = new ReflectDatumReader<>(CLUB_SCHEMA_READ);
        DataFileReader<Club> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isTrue();
        Club deserializedClub = dataFileReader.next(new Club());
        assertThat(deserializedClub.getFullName()).isEqualTo("RC Lens");
        assertThat(deserializedClub.getDisappearanceYear()).isNull();
        assertThat(deserializedClub.getFoundationYear()).isEqualTo(1906);
    }

    @Test
    public void should_correctly_deserialize_two_clubs() throws IOException {
        File outputClubsFile = new File(TWO_TEAMS_AVRO);

        DatumReader<Club> datumReader = new ReflectDatumReader<>(CLUB_SCHEMA_READ);
        DataFileReader<Club> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isTrue();
        Club rcLens = dataFileReader.next(new Club());
        Club lilleOsc = dataFileReader.next(new Club());
        assertThat(rcLens.getFullName()).isEqualTo("RC Lens");
        assertThat(rcLens.getDisappearanceYear()).isNull();
        assertThat(rcLens.getFoundationYear()).isEqualTo(1906);
        assertThat(lilleOsc.getFullName()).isEqualTo("Lille OSC");
        assertThat(lilleOsc.getDisappearanceYear()).isNull();
        assertThat(lilleOsc.getFoundationYear()).isEqualTo(1944);
    }

    @Test(expected = NullPointerException.class)
    public void should_fail_deserializing_with_writer_schema() throws IOException {
        // NPE is caused by the fact that reader schema saves object with different names
        // that the names used by class fields (full_name instead of fullName etc)
        // ReflectDatumReader will cause an exception of similar content:
        /**
         * java.lang.NullPointerException
         * 	at org.apache.avro.reflect.ReflectData.setField(ReflectData.java:138)
         * 	at org.apache.avro.generic.GenericDatumReader.readField(GenericDatumReader.java:240)
         * 	at org.apache.avro.reflect.ReflectDatumReader.readField(ReflectDatumReader.java:310)
         * 	(...)
         */
        File outputClubsFile = new File(RC_LENS_AVRO);

        DatumReader<Club> datumReader = new ReflectDatumReader<>(CLUB_SCHEMA_WRITE);
        DataFileReader<Club> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        dataFileReader.hasNext();
        dataFileReader.next();
    }

    @Test(expected = AvroTypeException.class)
    public void should_fail_on_deserializing_because_java_class_doesn_t_match_with_fields() throws IOException {
        // other way to get Schema is:
        Schema schema = ReflectData.get().getSchema(Club.class);
        // However, it doesn't know aliases, so the conversion will fail with exceptions similar to
        /**
         * org.apache.avro.AvroTypeException: Found com.waitingforcode.model.Club,
         * expecting com.waitingforcode.model.Club, missing required field fullName
         * 	at org.apache.avro.io.ResolvingDecoder.doAction(ResolvingDecoder.java:292)
         * 	at org.apache.avro.io.parsing.Parser.advance(Parser.java:88)
         *  at org.apache.avro.io.ResolvingDecoder.readFieldOrder(ResolvingDecoder.java:130)
         */
        File outputClubsFile = new File(RC_LENS_AVRO);

        DatumReader<Club> datumReader = new ReflectDatumReader<>(schema);
        DataFileReader<Club> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isTrue();
        Club deserializedClub = dataFileReader.next(new Club());
        assertThat(deserializedClub.getFullName()).isEqualTo("RC Lens");
        assertThat(deserializedClub.getDisappearanceYear()).isNull();
        assertThat(deserializedClub.getFoundationYear()).isEqualTo(1906);
    }

    @Test
    public void should_deserialize_schema_for_generic_data() throws IOException {
        File outputClubsFile = new File(RC_LENS_AVRO);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(CLUB_SCHEMA_WRITE);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isTrue();
        GenericRecord record = dataFileReader.next();
        // Notice that textual data stored in serialized files are not Java's String but
        // Avro's Utf8 objects. The difference between them is that Utf8 is mutable
        // and it's more efficient on reading and writing
        assertThat(((Utf8)record.get("full_name")).toString()).isEqualTo("RC Lens");
        assertThat((int)record.get("foundation_year")).isEqualTo(1906);
        assertThat(record.get("disappearance_year")).isNull();
        // Aliases are useful in the case of reflection-based readers. But they cannot
        // be used as aliases for accessing fields through GenericRecord
        assertThat(record.get("fullName")).isNull();
    }

    @Test
    public void should_correctly_parse_manually_defined_json_with_specified_union_field() throws IOException {
        // Note that for fields of union type, corresponding JSON is nested object composed by type and value
        String badJsonFormat = "{\"fullName\": \"Old club\", \"foundationYear\": 1900, \"disappearanceYear\": {\"int\": 1990}}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(badJsonFormat.getBytes());

        JsonDecoder decoder = new DecoderFactory().jsonDecoder(CLUB_SCHEMA_READ, inputStream);
        DatumReader<Club> reader = new ReflectDatumReader<>(CLUB_SCHEMA_READ);
        Club club = reader.read(null, decoder);

        assertThat(club.getFullName()).isEqualTo("Old club");
        assertThat(club.getFoundationYear()).isEqualTo(1900);
        assertThat(club.getDisappearanceYear()).isEqualTo(1990);
    }

    @Test
    public void should_correctly_parse_manually_defined_json_with_null_union_value() throws IOException {
        String badJsonFormat = "{\"fullName\": \"Old club\", \"foundationYear\": 1900, \"disappearanceYear\": null}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(badJsonFormat.getBytes());

        JsonDecoder decoder = new DecoderFactory().jsonDecoder(CLUB_SCHEMA_READ, inputStream);
        DatumReader<Club> reader = new ReflectDatumReader<>(CLUB_SCHEMA_READ);
        Club club = reader.read(null, decoder);

        assertThat(club.getFullName()).isEqualTo("Old club");
        assertThat(club.getFoundationYear()).isEqualTo(1900);
        assertThat(club.getDisappearanceYear()).isNull();
    }

    @Test(expected = AvroTypeException.class)
    public void should_fail_on_reading_badly_defined_union_field_of_json() throws IOException {
        // Union field is bad defined here, the parse will fail - nested object is expected
        String badJsonFormat = "{\"fullName\": \"Old club\", \"foundationYear\": 1900, \"disappearanceYear\": 1990}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(badJsonFormat.getBytes());

        JsonDecoder decoder = new DecoderFactory().jsonDecoder(CLUB_SCHEMA_READ, inputStream);
        DatumReader<Club> reader = new ReflectDatumReader<>(CLUB_SCHEMA_READ);
        reader.read(null, decoder);
    }

    @Test(expected = AvroTypeException.class)
    public void should_fail_on_converting_schema_with_bad_package() throws IOException {
        File outputClubsFile = new File(RC_LENS_AVRO);

        DatumReader<Club> datumReader = new ReflectDatumReader<>(CLUB_BAD_SCHEMA_READ);
        DataFileReader<Club> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        dataFileReader.next(new Club());
    }

    @Test(expected = AvroTypeException.class)
    public void should_also_fail_on_converting_generic_record_from_bad_package() throws IOException {
        File outputClubsFile = new File(RC_LENS_AVRO);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(CLUB_BAD_SCHEMA_READ);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(outputClubsFile, datumReader);

        assertThat(dataFileReader.hasNext()).isTrue();
        dataFileReader.next();
    }

}

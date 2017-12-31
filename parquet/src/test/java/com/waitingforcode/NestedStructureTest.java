package com.waitingforcode;

import com.waitingforcode.model.Game;
import com.waitingforcode.model.Player;
import com.waitingforcode.model.PlayerNestedGames;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.tools.Main;
import org.apache.parquet.tools.command.DumpCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class NestedStructureTest {

    private static final String TEST_FILE_REQUIRED_FIELDS = "/tmp/nested_organization_d1_r1";

    private static final Path TEST_FILE_REQUIRED_FIELDS_PATH = new Path(TEST_FILE_REQUIRED_FIELDS);

    private static final String TEST_FILE_1_OPTIONAL_FIELD = "/tmp/nested_organization_d2_r1";

    private static final Path TEST_FILE_1_OPTIONAL_FIELD_PATH = new Path(TEST_FILE_1_OPTIONAL_FIELD);

    private static final String TEST_FILE_2_OPTIONAL_FIELDS = "/tmp/nested_organization_d3_r1";

    private static final Path TEST_FILE_2_OPTIONAL_FIELDS_PATH = new Path(TEST_FILE_2_OPTIONAL_FIELDS);

    private static final String TEST_FILE_2_LEVEL_NESTED_FIELDS = "/tmp/nested_organization_d1_r2";

    private static final Path TEST_FILE_2_LEVEL_NESTED_FIELDS_PATH = new Path(TEST_FILE_2_LEVEL_NESTED_FIELDS);

    @BeforeClass
    @AfterClass
    public static void prepareTests() {
        new File(TEST_FILE_REQUIRED_FIELDS).delete();
        new File(TEST_FILE_1_OPTIONAL_FIELD).delete();
        new File(TEST_FILE_2_OPTIONAL_FIELDS).delete();
        new File(TEST_FILE_2_LEVEL_NESTED_FIELDS).delete();
    }

    @Test
    public void should_detect_1_repetition_level_and_1_definition_level_for_required_nested_fields() throws Exception {
        Schema schema = SchemaBuilder.builder().record("Player").namespace("com.waitingforcode.model").fields()
                .name("games").type().array().items()
                .record("Game").fields()
                .name("name").type().stringType().noDefault()
                .endRecord()
                .noDefault()
                .endRecord();
        List<Player> players = getPlayers(4);
        ParquetWriter<Player> writer = getWriter(schema, TEST_FILE_REQUIRED_FIELDS_PATH);
        writePlayers(players, writer);
        writer.close();

        String details = getDetailsForFile(TEST_FILE_REQUIRED_FIELDS);

        assertThat(details).contains("games.array.name TV=12 RL=1 DL=1");
    }

    @Test
    public void should_detect_1_repetition_level_and_2_definition_levels_for_1_optional_nested_field() throws Exception {
        Schema schema = SchemaBuilder.builder().record("Player").namespace("com.waitingforcode.model").fields()
                .name("games").type().nullable().array().items()
                .record("Game").fields()
                .name("name").type().stringType().noDefault()
                .endRecord()
                .noDefault()
                .endRecord();
        List<Player> players = getPlayers(4);
        ParquetWriter<Player> writer = getWriter(schema, TEST_FILE_1_OPTIONAL_FIELD_PATH);
        writePlayers(players, writer);
        writer.close();

        String details = getDetailsForFile(TEST_FILE_1_OPTIONAL_FIELD);

        assertThat(details).contains("games.array.name TV=12 RL=1 DL=2");
    }


    @Test
    public void should_detect_1_repetition_level_and_2_definition_levels_for_2_optional_nested_fields() throws Exception {
        Schema schema = SchemaBuilder.builder().record("Player").namespace("com.waitingforcode.model").fields()
                .name("games").type().nullable().array().items()
                .record("Game").fields()
                .name("name").type().nullable().stringType().noDefault()
                .endRecord()
                .noDefault()
                .endRecord();
        List<Player> players = getPlayers(4);
        ParquetWriter<Player> writer = getWriter(schema, TEST_FILE_2_OPTIONAL_FIELDS_PATH);
        writePlayers(players, writer);
        writer.close();

        String details = getDetailsForFile(TEST_FILE_2_OPTIONAL_FIELDS);

        assertThat(details).contains("games.array.name TV=12 RL=1 DL=3");
    }

    @Test
    public void should_detect_2_repetition_levels_and_2_definition_levels_for_required_twice_nested_fields() throws Exception {
        List<PlayerNestedGames> players = getPlayersForNestedGame(4);
        ParquetWriter<PlayerNestedGames> writer = getWriter(ReflectData.get().getSchema(PlayerNestedGames.class),
                TEST_FILE_2_LEVEL_NESTED_FIELDS_PATH);
        writePlayers(players, writer);
        writer.close();

        String details = getDetailsForFile(TEST_FILE_2_LEVEL_NESTED_FIELDS);

        assertThat(details).contains("games.array.array.name TV=12 RL=2 DL=2");
    }

    private static <T> ParquetWriter<T> getWriter(Schema schema, Path filePath) throws IOException {
        return AvroParquetWriter.<T>builder(filePath)
                .enableDictionaryEncoding()
                .withSchema(schema)
                .withDataModel(ReflectData.get())
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .build();
    }

    private static String getDetailsForFile(String fileName) throws Exception {
        GnuParser gnuParser = new GnuParser();
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteOutputStream);
        Main.out = printStream;
        DumpCommand dumpCommand = new DumpCommand();
        CommandLine commandLineJsonOutput = gnuParser.parse(new Options(), new String[]{fileName});
        dumpCommand.execute(commandLineJsonOutput);
        return new String(byteOutputStream.toByteArray());
    }

    private static <T> void writePlayers(List<T> players, ParquetWriter<T> writer) throws IOException {
        for (int i = 0; i < players.size(); i++) {
            writer.write(players.get(i));
        }
    }

    private static List<Player> getPlayers(int playersNumber) {
        List<Player> players = new ArrayList<>();
        for (int i = 0; i < playersNumber; i++) {
            Player player = Player.valueOf(
                    Game.ofName("Game#1_"+i), Game.ofName("Game#2_"+i), Game.ofName("Game#3_"+i)
            );
            players.add(player);
        }
        return players;
    }

    private static List<PlayerNestedGames> getPlayersForNestedGame(int playersNumber) {
        List<PlayerNestedGames> players = new ArrayList<>();
        for (int i = 0; i < playersNumber; i++) {
            PlayerNestedGames player = PlayerNestedGames.valueOf(
                    Game.ofName("Game#1_"+i), Game.ofName("Game#2_"+i), Game.ofName("Game#3_"+i)
            );
            players.add(player);
        }
        return players;
    }

}

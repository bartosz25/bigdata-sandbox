package com.waitingforcode;

import com.waitingforcode.model.Civilities;
import com.waitingforcode.model.WorkingCitizen;
import com.waitingforcode.model.WorkingCitizenCreator;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static com.waitingforcode.model.WorkingCitizen.AVRO_SCHEMA;
import static com.waitingforcode.parquet.Filters.getMetadataForColumn;
import static org.assertj.core.api.Assertions.assertThat;

public class FileOrganizationTest {

    private static final String TEST_FILE = "/tmp/file_organization";

    private static final Path TEST_FILE_PATH = new Path(TEST_FILE);

    @BeforeClass
    public static void createContext() throws IOException {
        new File(TEST_FILE).delete();
        WorkingCitizen workingCitizen1 = WorkingCitizenCreator.getSampleWorkingCitizen(Civilities.MISS);
        WorkingCitizen workingCitizen2 = WorkingCitizenCreator.getSampleWorkingCitizen(Civilities.MR);
        ParquetWriter<WorkingCitizen> writer = AvroParquetWriter.<WorkingCitizen>builder(TEST_FILE_PATH)
                .enableDictionaryEncoding()
                .withSchema(AVRO_SCHEMA)
                .withDataModel(ReflectData.get())
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .build();
        writer.write(workingCitizen1);
        writer.write(workingCitizen2);
        writer.close();
    }

    @AfterClass
    public static void deleteFile() {
        new File(TEST_FILE).delete();
    }

    @Test
    public void should_get_correct_row_group_information() throws IOException {
        ParquetFileReader fileReader = ParquetFileReader.open(new Configuration(), TEST_FILE_PATH);

        List<BlockMetaData> rowGroups = fileReader.getRowGroups();

        assertThat(rowGroups).hasSize(1);
        BlockMetaData rowGroup = rowGroups.get(0);
        // We test only against several fields
        ColumnChunkMetaData civility = getMetadataForColumn(rowGroup, "civility");
        // It varies, sometimes it's 352, 356 or 353 - so do not assert on it
        // Only show that the property exists
        long offset = civility.getFirstDataPageOffset();
        assertThat(civility.getFirstDataPageOffset()).isEqualTo(offset);
        assertThat(civility.getStartingPos()).isEqualTo(offset);
        assertThat(civility.getCodec()).isEqualTo(CompressionCodecName.UNCOMPRESSED);
        assertThat(civility.getEncodings()).contains(Encoding.DELTA_BYTE_ARRAY);
        assertThat(civility.getValueCount()).isEqualTo(2);
        assertThat(civility.getTotalSize()).isEqualTo(60L);
        assertThat(civility.getType()).isEqualTo(PrimitiveType.PrimitiveTypeName.BINARY);
        // CHeck credit rating to see stats
        ColumnChunkMetaData creditRating = getMetadataForColumn(rowGroup, "creditRating");
        assertThat(creditRating.getStatistics()).isNotNull();
        // Both have random values, so no to assert on exact values
        assertThat(creditRating.getStatistics().genericGetMax()).isNotNull();
        assertThat(creditRating.getStatistics().genericGetMin()).isNotNull();
        assertThat(creditRating.getStatistics().hasNonNullValue()).isTrue();
        assertThat(creditRating.getStatistics().getNumNulls()).isEqualTo(0);
    }

    @Test
    public void should_read_footer_of_correctly_written_file() throws IOException, URISyntaxException {
        ParquetFileReader fileReader = ParquetFileReader.open(new Configuration(), TEST_FILE_PATH);
        ParquetMetadata footer = fileReader.getFooter();

        org.apache.parquet.hadoop.metadata.FileMetaData footerMetadata = footer.getFileMetaData();

        assertThat(footerMetadata.getKeyValueMetaData()).containsKeys("parquet.avro.schema", "writer.model.name");
        assertThat(footerMetadata.getCreatedBy())
                .isEqualTo("parquet-mr version 1.9.0 (build 38262e2c80015d0935dad20f8e18f2d6f9fbd03c)");
        StringBuilder schemaStringifier = new StringBuilder();
        footerMetadata.getSchema().writeToStringBuilder(schemaStringifier, "");
        assertThat(schemaStringifier.toString().replaceAll("\n", "").replaceAll("  ", "")).isEqualTo(
                "message com.waitingforcode.model.WorkingCitizen {" +
                    "required group professionalSkills (LIST) {"+
                        "repeated binary array (UTF8);"+
                    "}"+
                    "required group professionsPerYear (MAP) {"+
                        "repeated group map (MAP_KEY_VALUE) {"+
                            "required binary key (UTF8);"+
                            "required binary value (UTF8);"+
                        "}"+
                    "}"+
                    "required binary civility (ENUM);"+
                    "required binary firstName (UTF8);"+
                    "required binary lastName (UTF8);"+
                    "required double creditRating;"+
                    "required boolean isParent;"+
                "}");
    }


}

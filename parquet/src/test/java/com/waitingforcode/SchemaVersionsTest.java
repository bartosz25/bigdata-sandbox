package com.waitingforcode;

import com.waitingforcode.model.Civilities;
import com.waitingforcode.model.WorkingCitizen;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.waitingforcode.model.WorkingCitizen.AVRO_SCHEMA;
import static com.waitingforcode.model.WorkingCitizenCreator.getSampleWorkingCitizen;
import static com.waitingforcode.parquet.Filters.getMetadataForColumn;
import static org.assertj.core.api.Assertions.assertThat;

public class SchemaVersionsTest {

    private static final String TEST_FILE_V1 = "/tmp/schema_versions_v1";
    private static final String TEST_FILE_V2 = "/tmp/schema_versions_v2";

    private static final WorkingCitizen WORKING_CITIZEN_1 = getSampleWorkingCitizen(Civilities.MISS, 100);
    private static final WorkingCitizen WORKING_CITIZEN_2 = getSampleWorkingCitizen(Civilities.MR, 200);

    @BeforeClass
    @AfterClass
    public static void createContext() throws IOException {
        new File(TEST_FILE_V1).delete();
        new File(TEST_FILE_V2).delete();
    }

    @Test
    public void should_compare_files_written_with_both_available_versions() throws IOException {
        Path filePathV1 = new Path(TEST_FILE_V1);
        writeCitizens(filePathV1, ParquetProperties.WriterVersion.PARQUET_1_0);
        Path filePathV2 = new Path(TEST_FILE_V2);
        writeCitizens(filePathV2, ParquetProperties.WriterVersion.PARQUET_2_0);

        ParquetFileReader fileReaderV1 = ParquetFileReader.open(new Configuration(), filePathV1);
        ParquetFileReader fileReaderV2 = ParquetFileReader.open(new Configuration(), filePathV2);

        List<BlockMetaData> rowGroupsV1 = fileReaderV1.getRowGroups();
        BlockMetaData rowGroupV1 = rowGroupsV1.get(0);
        List<BlockMetaData> rowGroupsV2 = fileReaderV2.getRowGroups();
        BlockMetaData rowGroupV2 = rowGroupsV2.get(0);
        // Check double value
        ColumnChunkMetaData creditRatingV1 = getMetadataForColumn(rowGroupV1, "creditRating");
        ColumnChunkMetaData creditRatingV2 = getMetadataForColumn(rowGroupV2, "creditRating");
        assertThat(creditRatingV1.getEncodings()).isNotEqualTo(creditRatingV2.getEncodings());
        assertThat(creditRatingV1.getEncodings()).contains(Encoding.BIT_PACKED, Encoding.PLAIN);
        assertThat(creditRatingV2.getEncodings()).contains(Encoding.PLAIN);
        // Check nested type
        ColumnChunkMetaData professionalSkillsV1 = getMetadataForColumn(rowGroupV1, "professionalSkills");
        ColumnChunkMetaData professionalSkillsV2 = getMetadataForColumn(rowGroupV2, "professionalSkills");
        assertThat(professionalSkillsV1.getEncodings()).isNotEqualTo(professionalSkillsV2.getEncodings());
        assertThat(professionalSkillsV1.getEncodings()).contains(Encoding.PLAIN_DICTIONARY, Encoding.RLE);
        assertThat(professionalSkillsV2.getEncodings()).contains(Encoding.RLE_DICTIONARY, Encoding.PLAIN);
        // Check enum type
        ColumnChunkMetaData civilityV1 = getMetadataForColumn(rowGroupV1, "civility");
        ColumnChunkMetaData civilityV2 = getMetadataForColumn(rowGroupV2, "civility");
        assertThat(civilityV1.getEncodings()).isNotEqualTo(civilityV2.getEncodings());
        assertThat(civilityV1.getEncodings()).contains(Encoding.BIT_PACKED, Encoding.PLAIN);
        assertThat(civilityV2.getEncodings()).contains(Encoding.DELTA_BYTE_ARRAY);
    }

    private static void writeCitizens(Path filePath, ParquetProperties.WriterVersion version) throws IOException {
        ParquetWriter<WorkingCitizen> writer = AvroParquetWriter.<WorkingCitizen>builder(filePath)
                .enableDictionaryEncoding()
                .withSchema(AVRO_SCHEMA)
                .withDataModel(ReflectData.get())
                .withWriterVersion(version)
                .build();
        writer.write(WORKING_CITIZEN_1);
        writer.write(WORKING_CITIZEN_2);
        writer.close();

    }

}

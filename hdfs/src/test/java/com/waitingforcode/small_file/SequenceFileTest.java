package com.waitingforcode.small_file;

import com.waitingforcode.HdfsConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SequenceFileTest {

    private FileSystem fileSystem = HdfsConfiguration.getFileSystem();

    @Before
    public void openFileSystem() throws IOException {
        fileSystem = HdfsConfiguration.getFileSystem();
        FileUtils.writeStringToFile(new File("./1.txt"), "Test1");
        FileUtils.writeStringToFile(new File("./2.txt"), "Test2");
        FileUtils.writeStringToFile(new File("./3.txt"), "Test3");
    }

    @After
    public void closeFileSystem() throws IOException {
        fileSystem.close();
        FileUtils.deleteQuietly(new File("./1.txt"));
        FileUtils.deleteQuietly(new File("./2.txt"));
        FileUtils.deleteQuietly(new File("./3.txt"));
    }

    @Test
    public void should_merge_3_small_text_files_to_one_sequence_file() throws IOException {
        Path filePath = new Path("sequence_file_example");
        SequenceFile.Writer writer = SequenceFile.createWriter(HdfsConfiguration.get(),
                SequenceFile.Writer.file(filePath), SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class));
        for (int i = 1; i <= 3; i++) {
            String fileName = i+".txt";
            writer.append(new Text(fileName), new Text(FileUtils.readFileToString(new File("./"+fileName))));
        }
        writer.close();

        SequenceFile.Reader reader = new SequenceFile.Reader(HdfsConfiguration.get(),
                SequenceFile.Reader.file(filePath));

        Text value = new Text();
        Text key = new Text();
        String[] keys = new String[3];
        String[] values = new String[3];
        int i = 0;
        while (reader.next(key, value)) {
            keys[i] = key.toString();
            values[i] = value.toString();
            i++;
        }
        reader.close();
        assertThat(keys).containsOnly("1.txt", "2.txt", "3.txt");
        assertThat(values).containsOnly("Test1", "Test2", "Test3");
    }

}

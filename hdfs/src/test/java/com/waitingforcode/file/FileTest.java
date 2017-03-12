package com.waitingforcode.file;

import com.waitingforcode.HdfsConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

public class FileTest {

    private FileSystem fileSystem = HdfsConfiguration.getFileSystem();

    @Before
    public void openFileSystem() {
        fileSystem = HdfsConfiguration.getFileSystem();
    }

    @After
    public void closeFileSystem() throws IOException {
        fileSystem.close();
    }

    @Test
    public void should_put_text_file_on_hdfs() throws IOException {
        Path fileToCreate = new Path("/hello_world.txt");
        FSDataOutputStream helloWorldFile = fileSystem.create(fileToCreate);
        helloWorldFile.writeUTF("Hello world");
        helloWorldFile.close();

        assertThat(fileSystem.exists(fileToCreate)).isTrue();
    }

    @Test
    public void should_fail_on_creating_file_with_block_smaller_than_configured_minimum() throws IOException {
        Path fileToCreate = new Path("/hello_world_custom.txt");
        boolean overrideExistent = true;
        // in bytes
        int bufferSize = 4096;
        short replicationFactor = 1;
        // in bytes
        long blockSize = 10L;
        try {
            fileSystem.create(fileToCreate, overrideExistent, bufferSize, replicationFactor, blockSize);
            fail("File creation should fail when the block size is lower than specified minimum");
        } catch (RemoteException re) {
            assertThat(re.getMessage()).contains("Specified block size is less than configured minimum value");
        }
    }

    @Test
    public void should_create_file_with_custom_block_size_and_replication_factor() throws IOException {
        Path fileToCreate = new Path("/hello_world_custom.txt");
        boolean overrideExistent = true;
        // in bytes
        int bufferSize = 4096;
        short replicationFactor = 2;
        // in bytes (twice minimum block size)
        long blockSize = 1048576L * 2L;
        FSDataOutputStream helloWorldFile = fileSystem.create(fileToCreate, overrideExistent, bufferSize, replicationFactor, blockSize);
        helloWorldFile.writeUTF("Hello world");
        helloWorldFile.close();

        assertThat(fileSystem.exists(fileToCreate)).isTrue();
        FileStatus status = fileSystem.getFileStatus(fileToCreate);
        assertThat(status.getReplication()).isEqualTo(replicationFactor);
        assertThat(status.getBlockSize()).isEqualTo(blockSize);
    }

    @Test
    public void should_create_sequence_file() throws IOException {
        Path filePath = new Path("sequence_file_example");
        Writer writer = SequenceFile.createWriter(HdfsConfiguration.get(),
                Writer.file(filePath), Writer.keyClass(IntWritable.class),
                Writer.valueClass(Text.class));
        writer.append(new IntWritable(1), new Text("A"));
        writer.append(new IntWritable(2), new Text("B"));
        writer.append(new IntWritable(3), new Text("C"));
        writer.close();

        SequenceFile.Reader reader = new SequenceFile.Reader(HdfsConfiguration.get(),
                SequenceFile.Reader.file(filePath));

        Text value = new Text();
        IntWritable key = new IntWritable();
        int[] keys = new int[3];
        String[] values = new String[3];
        int i = 0;
        while (reader.next(key, value)) {
            keys[i] = key.get();
            values[i] = value.toString();
            i++;
        }
        reader.close();
        assertThat(keys).containsOnly(1, 2, 3);
        assertThat(values).containsOnly("A", "B", "C");
    }


}

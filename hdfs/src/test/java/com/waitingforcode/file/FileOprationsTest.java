package com.waitingforcode.file;

import com.waitingforcode.HdfsConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.assertj.core.api.Assertions.assertThat;

public class FileOprationsTest {

    private static final boolean OVERRIDE_FILE = true;

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
    public void should_create_file_in_hdfs() throws IOException {
        Path fileToCreate = new Path("/test_1.txt");
        FSDataOutputStream helloWorldFile = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        helloWorldFile.close();

        assertThat(fileSystem.exists(fileToCreate)).isTrue();
    }

    @Test
    public void should_copy_file_from_local_file_system_to_hdfs_and_keep_local_file_unchanged() throws IOException {
        String filePath = FileOprationsTest.class.getClassLoader().getResource("hello_world.txt").getPath();
        File localFile = new File(filePath);
        boolean deleteSourceFile = false;
        boolean overrideTargetFile = true;
        Path copiedFilePath = new Path(localFile.getAbsolutePath());
        Path[] sourceFiles = new Path[] {copiedFilePath};
        Path targetPath = new Path("/");

        fileSystem.copyFromLocalFile(deleteSourceFile, overrideTargetFile, sourceFiles, targetPath);

        assertThat(localFile).exists();
        // As you can see, file is not copied with all absolute path
        // It's put in the root directory, as specified in targetPath local variable
        assertThat(localFile.getAbsolutePath()).isNotEqualTo("/hello_world.txt");
        assertThat(fileSystem.exists(new Path("/hello_world.txt"))).isTrue();
    }

    @Test
    public void should_copy_file_from_local_file_system_to_hdfs_and_delete_local_file() throws IOException {
        File localFile = new File("/tmp/text.txt");
        FileUtils.writeStringToFile(localFile, "Test content");
        boolean deleteSourceFile = true;
        boolean overrideTargetFile = true;
        Path copiedFilePath = new Path(localFile.getAbsolutePath());
        Path[] sourceFiles = new Path[] {copiedFilePath};
        Path targetPath = new Path("/");

        fileSystem.copyFromLocalFile(deleteSourceFile, overrideTargetFile, sourceFiles, targetPath);

        assertThat(localFile).doesNotExist();
        // As in should_copy_file_from_local_file_system_to_hdfs_and_keep_local_file_unchanged,
        // file is copied in the root directory
        assertThat(fileSystem.exists(new Path("/text.txt"))).isTrue();
    }

    @Test
    public void should_move_file_from_local_file_system_to_hdfs() throws IOException {
        File localFile = new File("/tmp/hdfs_file_to_copy.txt");
        localFile.createNewFile();
        Path sourcePath = new Path(localFile.getAbsolutePath());
        Path targetPath = new Path("/");

        fileSystem.moveFromLocalFile(sourcePath, targetPath);

        assertThat(localFile).doesNotExist();
        assertThat(fileSystem.exists(new Path("/hdfs_file_to_copy.txt"))).isTrue();
    }

    @Test
    public void should_read_whole_file_content() throws IOException {
        Path fileToCreate = new Path("/test_2.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.writeUTF("sample");
        writeStream.writeUTF(" ");
        writeStream.writeUTF("content");
        writeStream.close();

        FSDataInputStream readStream = fileSystem.open(fileToCreate);

        String fileContent = readStream.readUTF() + readStream.readUTF() + readStream.readUTF();
        assertThat(fileContent).isEqualTo("sample content");
    }

    @Test
    public void should_change_file_content_in_hdfs() throws IOException {
        Path fileToCreate = new Path("/test_2.txt");
        short replicationFactor = 1;
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, replicationFactor);
        writeStream.writeUTF("Test_1;");
        writeStream.close();
        FSDataOutputStream appendStream = fileSystem.append(fileToCreate);
        appendStream.writeUTF("Test_2");
        appendStream.close();

        FSDataInputStream readStream = fileSystem.open(fileToCreate);

        String fileContent = readStream.readUTF()+readStream.readUTF();
        assertThat(fileContent).isEqualTo("Test_1;Test_2");
    }

    @Test
    public void should_rename_hdfs_file() throws IOException {
        Path fileToCreate = new Path("/test_3.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.close();
        assertThat(fileSystem.exists(fileToCreate)).isTrue();
        Path renamedPath = new Path("/renamed_test_3.txt");

        boolean wasRenamed = fileSystem.rename(fileToCreate, renamedPath);

        assertThat(wasRenamed).isTrue();
        assertThat(fileSystem.exists(fileToCreate)).isFalse();
        assertThat(fileSystem.exists(renamedPath)).isTrue();
    }

    @Test(expected = ClosedChannelException.class)
    public void should_fail_on_changing_file_content_after_closing_it() throws IOException {
        Path fileToCreate = new Path("/test_3.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.writeUTF("Test_1;");
        writeStream.close();
        writeStream.writeUTF("Should fail now because stream was already closed");
    }

    @Test
    public void should_seek_file_content() throws IOException {
        Path fileToCreate = new Path("/test_4.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.writeUTF("abcdefghijklmnoprst");
        writeStream.close();

        FSDataInputStream readStream = fileSystem.open(fileToCreate);

        readStream.seek(7L);
        int nextByte;
        String fileContent = "";
        while((nextByte = readStream.read()) != -1) {
            fileContent += (char)nextByte;
        }
        // seek() makes that file is read from
        assertThat(fileContent).isEqualTo("fghijklmnoprst");
    }

    @Test(expected = EOFException.class)
    public void should_fail_with_seek_and_fully_read() throws IOException {
        Path fileToCreate = new Path("/test_5.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.writeUTF("abcdefghijklmnoprst");
        writeStream.close();

        FSDataInputStream readStream = fileSystem.open(fileToCreate);

        readStream.seek(1L);
        readStream.readUTF();
    }

    @Test
    public void should_delete_file_in_hdfs() throws IOException {
        Path fileToDeletePath = new Path("/test_1.txt");
        FSDataOutputStream helloWorldFile = fileSystem.create(fileToDeletePath, OVERRIDE_FILE);
        helloWorldFile.close();
        boolean deleteRecursive = false;
        assertThat(fileSystem.exists(fileToDeletePath)).isTrue();

        boolean fileDeleted = fileSystem.delete(fileToDeletePath, deleteRecursive);
        assertThat(fileDeleted).isTrue();
        assertThat(fileSystem.exists(fileToDeletePath)).isFalse();
    }

    @Test
    public void should_delete_file_moved_from_local_file_system_to_hdfs() throws IOException {
        // Here we expect only HDFS file to be deleted
        File localFile = new File("/tmp/hdfs_file_to_copy.txt");
        localFile.createNewFile();
        boolean deleteSourceFile = false;
        boolean overrideTargetFile = true;
        Path copiedFilePath = new Path(localFile.getAbsolutePath());
        Path[] sourceFiles = new Path[] {copiedFilePath};
        Path targetPath = new Path("/");

        fileSystem.copyFromLocalFile(deleteSourceFile, overrideTargetFile, sourceFiles, targetPath);

        assertThat(localFile).exists();
        Path hdfsPath = new Path("/hdfs_file_to_copy.txt");
        assertThat(fileSystem.exists(hdfsPath)).isTrue();
        boolean recursiveDelete = false;
        assertThat(fileSystem.delete(hdfsPath, recursiveDelete)).isTrue();
        assertThat(fileSystem.exists(hdfsPath)).isFalse();
        assertThat(localFile).exists();
    }

    @Test
    public void should_get_file_stats() throws IOException {
        long beforeAccessTime = System.currentTimeMillis();
        Path fileToCreate = new Path("/test_6.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.writeUTF("1");
        writeStream.close();
        long afterWriteTime = System.currentTimeMillis();

        FileStatus fileStatus = fileSystem.getFileStatus(fileToCreate);

        short defaultReplication = Short.valueOf(HdfsConfiguration.get().get(DFS_REPLICATION_KEY));
        long blockSize = Long.valueOf(HdfsConfiguration.get().get(DFS_BLOCK_SIZE_KEY));
        assertThat(fileStatus.getReplication()).isEqualTo(defaultReplication);
        assertThat(fileStatus.isFile()).isTrue();
        assertThat(fileStatus.isDirectory()).isFalse();
        assertThat(fileStatus.isSymlink()).isFalse();
        assertThat(fileStatus.getBlockSize()).isEqualTo(blockSize);
        assertThat(fileStatus.getAccessTime()).isGreaterThan(beforeAccessTime).isLessThan(afterWriteTime);
        assertThat(fileStatus.getGroup()).isEqualTo("supergroup");
    }

    @Test
    public void should_get_block_locations() throws IOException {
        Path fileToCreate = new Path("/test_7.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.writeBytes("1");
        writeStream.close();

        long fileBlockSize = fileSystem.getDefaultBlockSize(fileToCreate);
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileToCreate, 0L, fileBlockSize);

        assertThat(blockLocations).hasSize(1);
        assertThat(blockLocations[0].isCorrupt()).isFalse();
        assertThat(blockLocations[0].getLength()).isEqualTo(1L);
        assertThat(blockLocations[0].getHosts().length).isEqualTo(1);
        assertThat(blockLocations[0].getTopologyPaths()[0]).startsWith("/default-rack");
    }

    @Test
    public void should_get_empty_block_locations_for_empty_file() throws IOException {
        Path fileToCreate = new Path("/test_8.txt");
        FSDataOutputStream writeStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
        writeStream.close();

        long fileBlockSize = fileSystem.getDefaultBlockSize(fileToCreate);
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileToCreate, 0L, fileBlockSize);

        assertThat(blockLocations.length).isEqualTo(0);
    }

    @Test
    public void should_prove_that_utf_takes_more_place_than_string() throws IOException {
        Path fileToCreateBytes = new Path("/test_8_bytes.txt");
        Path fileToCreateUtf = new Path("/test_8_utf.txt");
        FSDataOutputStream bytesWriteStream = fileSystem.create(fileToCreateBytes, OVERRIDE_FILE);
        bytesWriteStream.writeBytes("123");
        bytesWriteStream.writeBytes("456");
        bytesWriteStream.close();
        FSDataOutputStream utfWriteStream = fileSystem.create(fileToCreateUtf, OVERRIDE_FILE);
        utfWriteStream.writeUTF("123");
        utfWriteStream.writeUTF("456");
        utfWriteStream.close();

        long fileBlockSizeByte = fileSystem.getDefaultBlockSize(fileToCreateBytes);
        BlockLocation[] blockLocationsByte = fileSystem.getFileBlockLocations(fileToCreateBytes, 0L, fileBlockSizeByte);
        long fileBlockSizeUtf = fileSystem.getDefaultBlockSize(fileToCreateUtf);
        BlockLocation[] blockLocationsUtf = fileSystem.getFileBlockLocations(fileToCreateUtf, 0L, fileBlockSizeUtf);

        // The difference in size comes from the fact that writeUTF adds always 2 bytes
        assertThat(blockLocationsUtf[0].getLength()).isEqualTo(10L);
        assertThat(blockLocationsByte[0].getLength()).isEqualTo(6L);
    }

}

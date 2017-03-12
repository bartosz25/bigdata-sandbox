package com.waitingforcode.file;

import com.waitingforcode.HdfsConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.waitingforcode.util.CollectionUtil.convertFilesIteratorToList;
import static org.assertj.core.api.Assertions.assertThat;

public class DirectoryOperationsTest {


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
    public void should_create_directory_in_hdfs() throws IOException {
        Path dirPath = new Path("/documents");
        boolean isDirCreated = fileSystem.mkdirs(dirPath);

        assertThat(isDirCreated).isTrue();
    }

    @Test
    public void should_create_directory_recursively_in_hdfs() throws IOException {
        Path dirPath = new Path("/articles/2000-01-01/validated");
        boolean isDirCreated = fileSystem.mkdirs(dirPath);

        assertThat(isDirCreated).isTrue();
        assertThat(fileSystem.exists(new Path("/articles"))).isTrue();
        assertThat(fileSystem.exists(new Path("/articles/2000-01-01"))).isTrue();
        assertThat(fileSystem.exists(new Path("/articles/2000-01-01/validated"))).isTrue();
    }

    @Test
    public void should_list_directory_content() throws IOException {
        Path dirPath = new Path("/documents");
        fileSystem.mkdirs(dirPath);
        for (int i = 0; i < 10; i++) {
            Path fileToCreate = new Path("/documents/file_"+i);
            FSDataOutputStream fileStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
            fileStream.close();
        }

        boolean recursive = false;
        List<LocatedFileStatus> files = convertFilesIteratorToList(fileSystem.listFiles(dirPath, recursive));

        assertThat(files).hasSize(10);
        List<String> fileNames = files.stream().map(file -> file.getPath().getName()).collect(Collectors.toList());
        assertThat(fileNames).containsOnly("file_0", "file_1", "file_2", "file_3", "file_4", "file_5", "file_6",
                "file_7", "file_8", "file_9");
    }

    @Test(expected = RemoteException.class)
    public void should_fail_on_deleting_non_empty_directory() throws IOException {
        Path dirPath = new Path("/trash");
        fileSystem.mkdirs(dirPath);
        for (int i = 0; i < 10; i++) {
            Path fileToCreate = new Path("/trash/file_"+i);
            FSDataOutputStream fileStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
            fileStream.close();
        }

        boolean recursive = false;
        fileSystem.delete(dirPath, recursive);
    }

    @Test
    public void should_delete_non_empty_directory_with_all_its_content() throws IOException {
        Path dirPath = new Path("/trash");
        fileSystem.mkdirs(dirPath);
        for (int i = 0; i < 10; i++) {
            Path fileToCreate = new Path("/trash/file_"+i);
            FSDataOutputStream fileStream = fileSystem.create(fileToCreate, OVERRIDE_FILE);
            fileStream.close();
        }

        boolean recursive = true;
        boolean wasDeleted = fileSystem.delete(dirPath, recursive);

        assertThat(wasDeleted).isTrue();
        assertThat(fileSystem.exists(dirPath)).isFalse();
    }

}

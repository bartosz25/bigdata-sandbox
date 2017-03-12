package com.waitingforcode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestHdfs {


    @Test
    public void ddd() throws IOException {
        URI uri = URI.create("hdfs://localhost:9000/");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(uri, configuration);

        for (int i = 0; i < 2; i++) {
            Path filePath = new Path("/test_"+(System.currentTimeMillis()+i)+".txt");
            fileSystem.create(filePath);
        }
        Path dir = new Path("/");
        RemoteIterator<LocatedFileStatus> filesIterator = fileSystem.listFiles(dir, false);
        while (filesIterator.hasNext()) {
            LocatedFileStatus file = filesIterator.next();

            System.out.println("file is " + file.toString());
        }

        System.out.println("X");

    }

}

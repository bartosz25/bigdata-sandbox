package com.waitingforcode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HdfsConfiguration {

    private static final Configuration CONFIGURATION = new Configuration();
    static {
        CONFIGURATION.set("fs.defaultFS", "hdfs://localhost:9000");
        CONFIGURATION.set("dfs.namenode.name.dir", "/tmp/hadoop/hdfs_dir/fsimage");
        CONFIGURATION.set("dfs.namenode.edits.dir", "/tmp/hdfs_dir/editlog");
        CONFIGURATION.set("dfs.datanode.data.dir", "/tmp/hdfs_dir/data_blocks");
        CONFIGURATION.set("dfs.support.append", "true");
    }

    public static Configuration get() {
        return CONFIGURATION;
    }

    public static FileSystem getFileSystem() {
        try {
            return FileSystem.get(CONFIGURATION);
        } catch (IOException ioe) {
            throw new IllegalStateException("HDFS file system couldn't be loaded", ioe);
        }
    }

}

package com.waitingforcode.util;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class CollectionUtil {

    private CollectionUtil() {
        // prevents init
    }

    public static List<LocatedFileStatus> convertFilesIteratorToList(RemoteIterator<LocatedFileStatus> files) throws IOException {
        List<LocatedFileStatus> materializedFiles = new ArrayList<>();
        while (files.hasNext()) {
            materializedFiles.add(files.next());
        }
        return materializedFiles;
    }

}

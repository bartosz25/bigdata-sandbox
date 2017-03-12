package com.waitingforcode.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class FileHelper {

    private FileHelper() {
        // prevents init
    }

    public static File readFileFromClasspath(String fileName) {
        return new File(FileHelper.class.getClassLoader().getResource(fileName).getFile());
    }

    public static void cleanDir(String dir) throws IOException {
        Path dirPath = Paths.get(dir);
        for (File file : dirPath.toFile().listFiles()) {
            file.delete();
        }
        Files.deleteIfExists(Paths.get(dir));
    }

}

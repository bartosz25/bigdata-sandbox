package com.waitingforcode;

import com.google.common.io.Files;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Paths;

public final class TestHelper {

    private TestHelper() {
        // prevents init
    }

    public static String readFromFile(String file) throws URISyntaxException, IOException {
        return Files.toString(
                Paths.get(TestHelper.class.getResource(file).toURI()).toFile(), Charset.forName("UTF-8")
        );
    }
}

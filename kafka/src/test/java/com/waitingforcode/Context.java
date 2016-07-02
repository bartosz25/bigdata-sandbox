package com.waitingforcode;

import java.io.IOException;
import java.util.Properties;

public class Context {

    private static final Context INSTANCE = new Context();

    public Properties getCommonProperties() throws IOException {
        Properties commonProperties = new Properties();
        commonProperties.load(Context.class.getClassLoader().getResourceAsStream("kafka.properties"));
        return commonProperties;
    }

    public static Context getInstance() {
        return INSTANCE;
    }

}

package com.waitingforcode.util;

import java.util.Properties;

public final class ProducerHelper {

    private ProducerHelper() {
        // prevents init
    }

    public static Properties decorateWithDefaults(Properties properties) {
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    public static String generateName(String topic, String context) {
        return "p_"+topic+context+System.currentTimeMillis();
    }

}

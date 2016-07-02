package com.waitingforcode.util;

import java.util.Properties;

public final class ConsumerHelper {

    private ConsumerHelper() {
        // prevents init
    }

    public static String generateName(String topic, String context) {
        return "c_"+topic+context+System.currentTimeMillis();
    }

    public static Properties decoratePropertiesWithDefaults(Properties properties, boolean autoCommit, Integer autoCommitInterval) {
        properties.setProperty("group.id", "wfc_integration_test");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("enable.auto.commit", ""+autoCommit);
        if (autoCommitInterval != null) {
            // TODO : transform to builder maybe
            properties.setProperty("auto.commit.interval.ms", autoCommitInterval.toString());
        }
        return properties;
    }

}

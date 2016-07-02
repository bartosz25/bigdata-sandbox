package com.waitingforcode.coordinator;

import com.waitingforcode.Context;
import com.waitingforcode.util.ConsumerHelper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class CoordinatorBasicTest {

    private static final String TOPIC_NAME = "basictopictest";

    @Test
    public void should_correctly_detect_coordinator_work() throws IOException, InterruptedException {
        System.out.println("[i] Before run, please try topic with below command:");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic basictopictest");
        Thread.sleep(10_000);

        TestAppender testAppender = new TestAppender();
        String PATTERN = "%m";
        testAppender.setLayout(new PatternLayout(PATTERN));
        testAppender.setThreshold(Level.TRACE);
        testAppender.activateOptions();
        Logger.getRootLogger().addAppender(testAppender);

        String testName = "test1_";
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("client.id", ConsumerHelper.generateName(TOPIC_NAME, testName));
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        localConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
        localConsumer.poll(500);

        assertThat(testAppender.getMessages()).hasSize(5);
        /**
         * Expected messages found in logs should be:
         * 1) Sending coordinator request for group wfc_integration_test to broker localhost:9092 (id: -1 rack: null)
         *
         * 2) Received group coordinator response ClientResponse(receivedTimeMs=1464517740784,
         *      disconnected=false, request=ClientRequest(expectResponse=true,
         *      callback=org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient$RequestFutureCompletionHandler@307f6b8c,
         *      request=RequestSend(header={api_key=10,api_version=0,correlation_id=0,
         *      client_id=c_basictopictesttest1_1464517739036}, body={group_id=wfc_integration_test}),
         *      createdTimeMs=1464517740433, sendTimeMs=1464517740744),
         *      responseBody={error_code=0,coordinator={node_id=0,host=bartosz-K70ID,port=9092}})
         *
         * 3) Discovered coordinator bartosz-K70ID:9092 (id: 2147483647 rack: null) for group wfc_integration_test.
         *
         * 4) Sending JoinGroup ({group_id=wfc_integration_test,session_timeout=30000,
         *    member_id=,protocol_type=consumer,group_protocols=[{protocol_name=range,
         *    protocol_metadata=java.nio.HeapByteBuffer[pos=0 lim=26 cap=26]}]})
         *    to coordinator bartosz-K70ID:9092 (id: 2147483647 rack: null)
         *
         * 5) Sending leader SyncGroup for group wfc_integration_test to coordinator
         *     bartosz-K70ID:9092 (id: 2147483647 rack: null): {
         *       group_id=wfc_integration_test,generation_id=1,
         *       member_id=c_basictopictesttest1_1464517739036-e96674a1-515a-42fa-9853-53566cb0e8f4,
         *       group_assignment=[
         *         {member_id=c_basictopictesttest1_1464517739036-e96674a1-515a-42fa-9853-53566cb0e8f4,
         *          member_assignment=java.nio.HeapByteBuffer[pos=0 lim=34 cap=34]
         *         }
         *        ]
         *    }
         */
        assertMessageMatching(testAppender.getMessages(), "coordinator={node_id=0,");
        assertMessageMatching(testAppender.getMessages(), "Sending JoinGroup");
        assertMessageMatching(testAppender.getMessages(), "Sending leader SyncGroup");
    }

    private void assertMessageMatching(List<String> messages, String pattern) {
        boolean found = false;
        for (String message : messages) {
            if (message.contains(pattern)) {
                found = true;
                break;
            }
        }
        assertThat(found).isTrue();
    }


    private static final class TestAppender extends ConsoleAppender {

        private List<String> messages = new ArrayList<>();

        @Override
        public void append(LoggingEvent event) {
            if (event.getRenderedMessage().contains("coordinator")) {
                messages.add(event.getRenderedMessage());
            }
        }

        public List<String> getMessages() {
            return messages;
        }
    }

}

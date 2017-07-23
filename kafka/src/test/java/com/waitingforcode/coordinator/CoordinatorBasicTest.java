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
    public void should_correctly_detect_coordinator_work() throws IOException {
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

        assertThat(testAppender.getMessages()).hasSize(3);
        /**
         * Expected messages found in logs should be:
         * 1) Group metadata response ClientResponse(receivedTimeMs=1463484813276,
         *    disconnected=false, request=ClientRequest(expectResponse=true,
         *    callback=org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient$RequestFutureCompletionHandler@7a1ebcd8,
         *    request=RequestSend(header={api_key=10,api_version=0,correlation_id=2,client_id=c_basictopictesttest1_1463484812981},
         *    body={group_id=wfc_integration_test}), createdTimeMs=1463484813273, sendTimeMs=1463484813275),
         *
         *    responseBody={error_code=0,coordinator={node_id=0,host=bartosz,port=9092}}),
         *
         * 2) Issuing request (JOIN_GROUP: {group_id=wfc_integration_test,session_timeout=30000,
         *    member_id=,protocol_type=consumer,group_protocols=[{protocol_name=range,
         *    protocol_metadata=java.nio.HeapByteBuffer[pos=0 lim=26 cap=26]}]}) to coordinator 2147483647
         *
         * 3) Issuing leader SyncGroup (SYNC_GROUP: {group_id=wfc_integration_test,generation_id=1,
         *    member_id=c_basictopictesttest1_1463484812981-eed8d595-396c-4349-9272-c8ffd796c86e,
         *    group_assignment=[{member_id=c_basictopictesttest1_1463484812981-eed8d595-396c-4349-9272-c8ffd796c86e,
         *    member_assignment=java.nio.HeapByteBuffer[pos=0 lim=34 cap=34]}]}) to coordinator 2147483647]
         */
        assertMessageMatching(testAppender.getMessages(), "coordinator={node_id=0,");
        assertMessageMatching(testAppender.getMessages(), "Issuing request (JOIN_GROUP");
        assertMessageMatching(testAppender.getMessages(), "Issuing leader SyncGroup");
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

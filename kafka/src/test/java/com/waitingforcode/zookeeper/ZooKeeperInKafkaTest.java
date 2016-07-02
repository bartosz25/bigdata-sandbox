package com.waitingforcode.zookeeper;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.waitingforcode.Context;
import com.waitingforcode.util.ConsumerHelper;
import com.waitingforcode.util.ZooKeeperHelper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class ZooKeeperInKafkaTest {

    private static final String TOPIC_NAME = "zktopic";

    private static final ZooKeeper ZK_INSTANCE;
    static {
        ZooKeeper zkTmp;
        try {
            zkTmp = ZooKeeperHelper.openSession();
        } catch (IOException|InterruptedException e) {
            e.printStackTrace();
            zkTmp = null;
        }
        ZK_INSTANCE = zkTmp;
    }

    @AfterClass
    public static void closeZkSession() throws InterruptedException {
        System.out.println("[i] Do not forget to remove topic with ");
        System.out.println("bin/kafka-topics.sh --delete --zookeeper localhost:2181  --topic "+TOPIC_NAME);
        System.out.println("[!] Only when you activated topic delete option 'delete.topic.enable = true'");
        Thread.sleep(10_000);
        ZK_INSTANCE.close();
    }

    @Test
    public void should_correctly_get_created_topic() throws InterruptedException, KeeperException, IOException {
        System.out.println("[i] Before launching the test, please create a topic called '"+TOPIC_NAME+"'");
        System.out.println("You can use this snippet: ");
        System.out.println("bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic "+TOPIC_NAME);
        Thread.sleep(10_000);

        ObjectMapper mapper = new ObjectMapper();
        /**
         * This query gets the information about topic called 'zktopic'. Among stored informations we
         * should retrieve:
         * - topic version - version of given topic (increased for example when new partition is added)
         * - partitions assigned - specified in '--partitions' parameter in creation command
         *
         * JSON output can look like this:
         * {"version":1,"partitions":{"0":[0]}}
         */
        String topicInfo = new String(ZK_INSTANCE.getData("/brokers/topics/"+TOPIC_NAME, false, new Stat()));
        System.out.println(topicInfo);
        JsonTopic topicData = mapper.readValue(topicInfo, JsonTopic.class);
        assertThat(topicData.version).isEqualTo(1);
        assertThat(topicData.partitions).hasSize(1).containsKey("0");
        assertThat(topicData.partitions.get("0")).hasSize(1).containsOnly(0);

        /**
         * Now, let's inspect some partition information. It contains more details than topic one because
         * it's more concerned about reads and writes.
         *
         * {"controller_epoch":1,"leader":0,"version":1,"leader_epoch":0,"isr":[0]}
         */
        String partitionInfo =
                new String(ZK_INSTANCE.getData("/brokers/topics/"+TOPIC_NAME+"/partitions/0/state", false, new Stat()));
        System.out.println(partitionInfo);
        JsonPartitionState partitionState = mapper.readValue(partitionInfo, JsonPartitionState.class);
        assertThat(partitionState.controllerEpoch).isEqualTo(1);
        assertThat(partitionState.leader).isEqualTo(0);
        assertThat(partitionState.version).isEqualTo(1);
        assertThat(partitionState.leaderEpoch).isEqualTo(0);
        assertThat(partitionState.isr).hasSize(1).containsOnly(0);

        /**
         * Now, let's increase the number of partitions to see if topic version
         * changes. It shouldn't change because of hard-coded '1' in AdminUtils.scala:
         * <pre>
         *   def getConfigChangeZnodeData(entityType: String, entityName: String) : Map[String, Any] = {
         *     Map("version" -> 1, "entity_type" -> entityType, "entity_name" -> entityName)
         *   }
         * </pre>
         *
         * The output should look like:
         * {"version":1,"partitions":{"1":[0],"0":[0]}}
         */
        System.out.println("[i] Please now add one partition to created topic, for example by using this query: ");
        System.out.println("bin/kafka-topics.sh --alter --zookeeper localhost:2181 --partitions 2 --topic "+TOPIC_NAME);
        Thread.sleep(10_000);
        String increasedTopicInfo = new String(ZK_INSTANCE.getData("/brokers/topics/"+TOPIC_NAME, false, new Stat()));
        System.out.println(increasedTopicInfo);
        JsonTopic increasedTopicData = mapper.readValue(increasedTopicInfo, JsonTopic.class);
        assertThat(increasedTopicData.version).isEqualTo(1);
        assertThat(increasedTopicData.partitions).hasSize(2).containsOnlyKeys("0", "1");
        assertThat(increasedTopicData.partitions.get("0")).hasSize(1).containsOnly(0);
        assertThat(increasedTopicData.partitions.get("1")).hasSize(1).containsOnly(0);

    }

    @Test
    public void should_correctly_get_consumer_information_when_two_consumers_want_to_consume_a_topic() throws IOException, InterruptedException, KeeperException {
        /**
         * Check if there are existent Offset Manager topic. If it exists, it can be removed through given steps:
         * 1) Delete /config/topics/__consumer_offsets
         * 2) Delete /brokers/topics/__consumer_offsets
         * 3) Delete local directory, for example:
         * rm -r /tmp/kafka-logs/__consumer_offsets-*
         * 4) Turn down Kafka
         * 5) Restart ZooKeeper
         * 6) Start Kafka
         * 7) Relaunch the test
         */
        try {
            new String(ZK_INSTANCE.getData("/brokers/topics/__consumer_offsets", false, new Stat()));
            fail("To see this test suceeds, you should first remove __consumer_offsets topic");
        } catch (KeeperException.NoNodeException kne) {
            // Do nothing
        }

        /**
         * Now when our test topic is created, let's create consumers of different groups subscribed
         * to it.
         * So first, let's create and subscribe consumers.
         */
        String group1 = "cons_group_1", group2 = "cons_group_2";
        String cons1Id = ConsumerHelper.generateName(TOPIC_NAME, "test1_"), cons2Id = ConsumerHelper.generateName(TOPIC_NAME, "test2_");
        Properties consumerProps = Context.getInstance().getCommonProperties();
        consumerProps.setProperty("group.id", group1);
        consumerProps.setProperty("client.id", cons1Id);
        KafkaConsumer<String, String> localConsumer =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps, false, null));
        Properties consumerProps2 = Context.getInstance().getCommonProperties();
        consumerProps2.setProperty("group.id", group2);
        consumerProps2.setProperty("client.id", cons2Id);
        KafkaConsumer<String, String> localConsumer2 =
                new KafkaConsumer<>(ConsumerHelper.decoratePropertiesWithDefaults(consumerProps2, false, null));

        TopicPartition partition = new TopicPartition(TOPIC_NAME, localConsumer.partitionsFor(TOPIC_NAME).get(0).partition());

        localConsumer.assign(Collections.singletonList(partition));
        localConsumer2.assign(Collections.singletonList(partition));

        // to create Offset Manager topic, we should want to get partition data, even if there are no
        // data currently
        localConsumer.poll(500);
        localConsumer2.poll(500);

        /**
         * Unlike low level consumers, consumers created directly from Java API use
         * Offset Manager to handle offsets. It's special topic called __consumer_offsets which should
         * appear after adding new consumers.
         * Sample response can be:
         * {"version":1,"partitions":{"45":[0],"34":[0],"12":[0],"8":[0],"19":[0],"23":[0],"4":[0],
         *              "40":[0],"15":[0],"11":[0],"9":[0],"44":[0],"33":[0],"22":[0],"26":[0],
         *              "37":[0],"13":[0],"46":[0],"24":[0],"35":[0],"16":[0],"5":[0],"10":[0],
         *              "48":[0],"21":[0],"43":[0],"32":[0],"49":[0],"6":[0],"36":[0],"1":[0],"
         *              39":[0],"17":[0],"25":[0],"14":[0],"47":[0],"31":[0],"42":[0],"0":[0],
         *              "20":[0],"27":[0],"2":[0],"38":[0],"18":[0],"30":[0],"7":[0],
         *              "29":[0],"41":[0],"3":[0],"28":[0]}}
         *
         * This output can change when offsets.topic.num.partitions is modified. It represents the number
         * of partitions created for the topic storing commit offsets. Each partition contains an array of
         * integers. These integers represent brokers ids. In our output we can only find 1 broker which id
         * is equal to 0.
         */
        ObjectMapper mapper = new ObjectMapper();
        String consumer1Info = new String(ZK_INSTANCE.getData("/brokers/topics/__consumer_offsets", false, new Stat()));
        JsonTopic increasedTopicData = mapper.readValue(consumer1Info, JsonTopic.class);
        assertThat(increasedTopicData.version).isEqualTo(1);
        assertThat(increasedTopicData.partitions).isNotEmpty();
    }

    private static final class JsonTopic {
        private Integer version;
        private Map<String, List<Integer>> partitions;

        public void setVersion(Integer version) {
            this.version = version;
        }

        public void setPartitions(Map<String, List<Integer>> partitions) {
            this.partitions = partitions;
        }
    }

    private static final class JsonPartitionState {
        @JsonProperty("controller_epoch")
        private Integer controllerEpoch;
        private Integer leader;
        private Integer version;
        @JsonProperty("leader_epoch")
        private Integer leaderEpoch;
        private List<Integer> isr;

        public void setControllerEpoch(Integer controllerEpoch) {
            this.controllerEpoch = controllerEpoch;
        }

        public void setLeader(Integer leader) {
            this.leader = leader;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }

        public void setLeaderEpoch(Integer leaderEpoch) {
            this.leaderEpoch = leaderEpoch;
        }

        public void setIsr(List<Integer> isr) {
            this.isr = isr;
        }
    }

    /**
     * Hello,
     *
     * I'm learning Kafka and currently I'm discovering its ZooKeeper layer. Somebody knows which is the purpose of
     * "version" field on /brokers/topics/${myTopicName} zNode ?
     *
     * I thought it was for handle different versions of the topic and it was increased at every change. But I was wrong
     * because after changing partitions from 1 to 2, the version is still "1".
     *
     * After looking at AdminUtils.scala I saw that the version is hard-coded:
     * def getConfigChangeZnodeData(entityType: String, entityName: String) : Map[String, Any] = {
     *   Map("version" -> 1, "entity_type" -> entityType, "entity_name" -> entityName)
     * }
     *
     *
     * Any information about the purpose of this field is welcome.
     *
     */

    /**
     * Scala consumer held in ZooKeeper:
     * bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic zktopic
     *
     * Java consumer doesn't - why so ?
     */

}

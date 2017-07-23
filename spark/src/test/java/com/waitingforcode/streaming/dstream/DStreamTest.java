package com.waitingforcode.streaming.dstream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerEvent;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

public class DStreamTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("DStream Test").setMaster("local[*]");
    private JavaSparkContext batchContext;
    private JavaStreamingContext streamingContext;
    private Queue<JavaRDD<String>> testQueue;

    @Before
    public void initContext() {
        batchContext = new JavaSparkContext(CONFIGURATION);
        streamingContext = new JavaStreamingContext(batchContext, Durations.milliseconds(3000));
        testQueue = new LinkedList<>();
    }

    @After
    public void stopContext() {
        streamingContext.stop(true);
    }

    @Test
    public void should_correctly_create_dstream_for_queue() throws IOException, InterruptedException {
        boolean oneAtTime = false;
        triggerDataCreation(4);

        // * Be careful - only data defined in the queue before stream declaration is read !
        //   It's because under-the-hood Spark crates its own queue and adds
        //   all RDDs already appended
        //   Since both are different objects, adding new RDD to original queue
        //   will not have any impact on the queue created by Spark
        //   See org.apache.spark.streaming.api.java.JavaStreamingContext.queueStream()
        // * When oneAtTime is true, it means that only 1 RDD will be read in each batch.
        //   By defining it at false, all queue's RDDs will be read in single batch interval
        JavaInputDStream<String> numbersStream = streamingContext.queueStream(testQueue, oneAtTime);

        JavaDStream<String> filteredNumbersStream = numbersStream.filter(number -> number.startsWith("Num"));

        // Create new 4 items to see that they won't be consumed
        triggerDataCreation(4);

        List<String> consumedRDDs = new ArrayList<>();
        filteredNumbersStream.foreachRDD(rdd -> rdd.collect().forEach(label -> consumedRDDs.add(label)));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(12_000L);

        // Tested queue should have 8 items but only 4, added before queueStream(...) call, was
        // consumed by Spark
        assertThat(testQueue).hasSize(8);
        // But only 4 items (the initial ones) were consumed
        assertThat(consumedRDDs).hasSize(4);
        assertThat(consumedRDDs).containsOnly(makeTestLabel(0), makeTestLabel(1), makeTestLabel(2), makeTestLabel(3));
    }

    @Test
    public void should_correctly_read_new_files_from_socket_stream() throws InterruptedException, IOException {
        int port = 8081;
        createSocketAndTriggerMessaging(port, 5);

        JavaDStream<String> socketDStream = streamingContext.socketTextStream("localhost", port, StorageLevel.MEMORY_ONLY());

        List<String> receivedData = new ArrayList<>();
        socketDStream.foreachRDD(rdd -> receivedData.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        assertThat(receivedData).hasSize(5);
        assertThat(receivedData).containsOnly(makeTestLabel(0), makeTestLabel(1), makeTestLabel(2),
                makeTestLabel(3), makeTestLabel(4));
    }

    @Test
    public void should_correctly_register_custom_streaming_listener() throws IOException, InterruptedException {
        boolean oneAtTime = false;
        triggerDataCreation(4);

        CustomStreamingListener listener = new CustomStreamingListener();
        streamingContext.addStreamingListener(listener);
        JavaInputDStream<String> numbersStream = streamingContext.queueStream(testQueue, oneAtTime);
        JavaDStream<String> filteredNumbersStream = numbersStream.filter(number -> number.startsWith("Num"));


        List<String> consumedRDDs = new ArrayList<>();
        filteredNumbersStream.foreachRDD(rdd -> rdd.collect().forEach(label -> consumedRDDs.add(label)));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(12_000L);

        assertThat(consumedRDDs).hasSize(4);
        assertThat(listener.getEvents()).hasSize(20);
        // The expected number of items depends strictly on created RDDs
        // In our case 4 RDDs are consumed, each one triggers 5 events (in order of triggering):
        // 1) Batch is submitted
        // 2) Batch is started
        // 3) Batch is completed
        // 4) Output operation is started
        // 5) Output operation is completed
        long batchSubmittedEvent =
                listener.getEvents().stream().filter(event -> event instanceof StreamingListenerBatchSubmitted).count();
        long batchStartedEvent =
                listener.getEvents().stream().filter(event -> event instanceof StreamingListenerBatchStarted).count();
        long batchCompletedEvents =
                listener.getEvents().stream().filter(event -> event instanceof StreamingListenerBatchCompleted).count();
        long outputOperationStartedEvents =
                listener.getEvents().stream().filter(event -> event instanceof StreamingListenerOutputOperationStarted).count();
        long outputOperationCompletedEvents =
                listener.getEvents().stream().filter(event -> event instanceof StreamingListenerOutputOperationCompleted).count();
        assertThat(batchSubmittedEvent).isEqualTo(4);
        assertThat(batchStartedEvent).isEqualTo(4);
        assertThat(batchCompletedEvents).isEqualTo(4);
        assertThat(outputOperationStartedEvents).isEqualTo(4);
        assertThat(outputOperationCompletedEvents).isEqualTo(4);
    }

    @Test
    public void should_correctly_work_with_receiver_stream() throws InterruptedException {
        JavaReceiverInputDStream<String> receiverInputDStream =
                streamingContext.receiverStream(new AutoDataMakingReceiver(StorageLevel.MEMORY_ONLY(), 3_000L, 2));

        List<JavaRDD<String>> batches = new ArrayList<>();
        List<String> receivedData = new ArrayList<>();
        receiverInputDStream.foreachRDD(rdd -> {
            List<String> collectedData = rdd.collect();
            if (!collectedData.isEmpty()) {
                batches.add(rdd);
                receivedData.addAll(collectedData);
            }
        });

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(15_000L);

        // Our receiver stores data in 2 sequences with 3 seconds interval
        // This interval corresponds to the batch interval. So we expect
        // to deal with 2 batches
        assertThat(batches).hasSize(2);
        assertThat(receivedData).hasSize(10);
        assertThat(receivedData).containsOnly(makeTestLabel(0), makeTestLabel(1), makeTestLabel(2),
                makeTestLabel(3), makeTestLabel(4));
    }

    @Test
    public void should_correctly_create_dstream_from_another_dstream() throws IOException, InterruptedException {
        boolean oneAtTime = true;
        triggerDataCreation(4);
        JavaInputDStream<String> numbersStream = streamingContext.queueStream(testQueue, oneAtTime);

        // Here we construct one DStream from another one. In occurrence,
        // constructed DStream (output stream) is built from input stream
        JavaDStream<String> filteredNumbersStream = numbersStream.filter(number -> number.startsWith("Num"));
        JavaPairDStream<String, Integer> pairWithCounterDStream =
                filteredNumbersStream.transformToPair(new StringRDDToPairRDDConverter());

        List<Tuple2<String, Integer>> consumedRDDs = new ArrayList<>();
        pairWithCounterDStream.foreachRDD(rdd -> consumedRDDs.addAll(rdd.collect()));

        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(10_000L);

        // Because 1 RDD is retrieved at time, the counter inside converter
        // will increment from 0 up to 3
        // To see the difference, make oneAtTime = false
        assertThat(consumedRDDs).hasSize(4);
        assertThat(consumedRDDs).containsOnly(new Tuple2<>(makeTestLabel(0), 0), new Tuple2<>(makeTestLabel(1), 1),
                new Tuple2<>(makeTestLabel(2), 2), new Tuple2<>(makeTestLabel(3), 3));
    }

    public static class AutoDataMakingReceiver extends Receiver<String> {

        private long sleepingTime;
        private int turns;

        public AutoDataMakingReceiver(StorageLevel storageLevel, long sleepingTime, int turns) {
            super(storageLevel);
            this.sleepingTime = sleepingTime;
            this.turns = turns;
        }

        @Override
        public void onStart() {
            for (int turn = 0; turn < turns; turn++) {
                for (int i = 0; i < 5; i++) {
                    store(makeTestLabel(i));
                }
                try {
                    Thread.sleep(sleepingTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onStop() {
            // Do nothing but you should clean data or close persistent connections here
        }
    }

    private static class CustomStreamingListener implements StreamingListener {

        private List<StreamingListenerEvent> events = new ArrayList<>();

        @Override
        public void onReceiverStarted(StreamingListenerReceiverStarted started) {
            events.add(started);
        }

        @Override
        public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
            events.add(receiverStopped);
        }

        @Override
        public void onReceiverError(StreamingListenerReceiverError receiverError) {
            events.add(receiverError);
        }

        @Override
        public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
            events.add(batchSubmitted);
        }

        @Override
        public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
            events.add(batchStarted);
        }

        @Override
        public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
            events.add(batchCompleted);
        }
        @Override
        public void onOutputOperationStarted(StreamingListenerOutputOperationStarted started) {
            events.add(started);
        }

        @Override
        public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted completed) {
            events.add(completed);
        }

        public List<StreamingListenerEvent> getEvents() {
            return events;
        }
    }

    private static class StringRDDToPairRDDConverter implements Function<JavaRDD<String>, JavaPairRDD<String, Integer>>, Serializable {

        private static final LongAdder ADDER = new LongAdder();

        @Override
        public JavaPairRDD<String, Integer> call(JavaRDD<String> inputRDD) throws Exception {
            int newIndex = ADDER.intValue();
            ADDER.increment();
            return inputRDD.mapToPair(entry -> new Tuple2<>(entry, newIndex));
        }
    }

    private void createSocketAndTriggerMessaging(int port, int labelsLimit) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(port);
                Socket socket = serverSocket.accept();

                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                for (int i = 0; i < labelsLimit; i++) {
                    writer.println(makeTestLabel(i));
                    try {
                        Thread.sleep(2_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            } finally {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }).start();
        latch.await(5, TimeUnit.SECONDS);
    }

    private void triggerDataCreation(int maxRDDs) throws IOException {
        for (int i = 0; i < maxRDDs; i++) {
            JavaRDD<String> newRDD1 = batchContext.parallelize(Arrays.asList(makeTestLabel(i)));
            testQueue.add(newRDD1);
        }
    }

    private static String makeTestLabel(int number) {
        return "Number " + number;
    }


}

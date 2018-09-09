package com.waitingforcode.computation;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class OnePhaseModelTest {

    private static class ReceivedMessagesConcatenator extends MessageCombiner<Long, String> {

        @Override
        public void combineMessages(MessageIterator<String> contentWithNumberMessages) {
            String sum = "";
            while (contentWithNumberMessages.hasNext()) {
                String receivedMessage = contentWithNumberMessages.next();
                sum += receivedMessage;
            }
            sendCombinedMessage(sum);
        }
    }

    private static class EdgesProcessor extends ComputeFunction<Long, String, String, String> {
        @Override
        public void compute(Vertex<Long, String> vertex,
                MessageIterator<String> messageIterator) {
            TestedGraph.Counter.ONE_PHASE.setExecution(getSuperstepNumber());
            String valueFromMessages = "";
            while (messageIterator.hasNext()) {
                valueFromMessages += messageIterator.next();
            }

            if (!valueFromMessages.isEmpty() || isTrunk(vertex)) {
                String value = valueFromMessages;
                getEdges().forEach(edge -> {
                    String valueToSend = value+edge.getValue();
                    sendMessageTo(edge.getTarget(), valueToSend);
                });
            }
            setNewVertexValue(TestedGraph.lettersToSorted(vertex.getValue() + valueFromMessages));
        }

        private boolean isTrunk(Vertex<Long, String> vertex) {
            return vertex.getId() == 1L || vertex.getId() == 7L;
        }
    }

    @Test
    public void should_traverse_graph_and_accumulate_edge_values() throws Exception {
        // Just for simpler debugging, set the parallelism level to 1
        VertexCentricConfiguration configuration = new VertexCentricConfiguration();
        configuration.setParallelism(1);
        Graph<Long, String, String> graph = TestedGraph.getGraph();

        // We define here a big number of iterations
        // At the end we'll see whether Gelly really computed the value in 20 iterations
        int maxIterations = 20;
        Graph<Long, String, String> result = graph.runVertexCentricIteration(new EdgesProcessor(),
                new ReceivedMessagesConcatenator(), maxIterations, configuration);

        List<Vertex<Long, String>> updatedVertices = result.getVertices().collect();
        assertThat(updatedVertices).containsAll(Arrays.asList(
                new Vertex<>(2L, "AC"), new Vertex<>(1L, ""), new Vertex<>(3L, "B"), new Vertex<>(7L, ""),
                new Vertex<>(6L, "BDF"), new Vertex<>(5L, "BDE"), new Vertex<>(4L, "BD")
        ));
        assertThat(TestedGraph.Counter.ONE_PHASE.get()).isEqualTo(4);
    }

}

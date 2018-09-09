package com.waitingforcode.computation;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TwoPhaseModelTest {

    @Test
    public void should_traverse_graph_and_accumulate_edge_values() throws Exception {
        // Just for simpler debugging, set the parallelism level to 1
        VertexCentricConfiguration configuration = new VertexCentricConfiguration();
        configuration.setParallelism(1);
        Graph<Long, String, String> graph = TestedGraph.getGraph();

        // We define here a big number of iterations
        // At the end we'll see whether Gelly really computed the value in 20 iterations
        int maxIterations = 20;

        Graph<Long, String, String> result = graph.runScatterGatherIteration(new Scatter(), new Gather(), maxIterations);

        List<Vertex<Long, String>> updatedVertices = result.getVertices().collect();
        assertThat(updatedVertices).containsAll(Arrays.asList(
                new Vertex<>(2L, "AC"), new Vertex<>(1L, ""), new Vertex<>(3L, "B"), new Vertex<>(7L, ""),
                new Vertex<>(6L, "BDF"), new Vertex<>(5L, "BDE"), new Vertex<>(4L, "BD")
        ));
        assertThat(TestedGraph.Counter.TWO_PHASES.get()).isEqualTo(4);
    }

    private static class Scatter extends ScatterFunction<Long, String, String, String> {

        @Override
        public void sendMessages(Vertex<Long, String> vertex) throws Exception {
            TestedGraph.Counter.TWO_PHASES.setExecution(getSuperstepNumber());
            // In the 1st superstep all vertices are considered as active
            // It's why here we process only trunks. Trunks trigger 1st level children that, in their turn, trigger
            // 2nd level children and so on. It explains why we can have a control on superstep number
            // It's only a little bit wasteful to have to filter trunks - it would be good to specify "starting nodes"
            // before executing the query
            if (isTrunk(vertex) || getSuperstepNumber() != 1) {
                for (Edge<Long, String> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), vertex.getValue()+edge.getValue());
                }
            }

        }

        private boolean isTrunk(Vertex<Long, String> vertex) {
            return vertex.getId() == 1 || vertex.getId() == 7; // TODO: put to commons
        }
    }

    private static class Gather extends GatherFunction<Long, String, String> {

        @Override
        public void updateVertex(Vertex<Long, String> vertex, MessageIterator<String> inMessages) throws Exception {
            String newValue = "";
            while (inMessages.hasNext()) {
                newValue += inMessages.next();
            }
            setNewVertexValue(TestedGraph.lettersToSorted(vertex.getValue() + newValue));
        }
    }

}

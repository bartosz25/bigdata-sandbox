package com.waitingforcode.computation;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TreePhaseModelTest {

    @Test
    public void should_traverse_graph_and_accumulate_edge_values() throws Exception {
        // Just for simpler debugging, set the parallelism level to 1
        VertexCentricConfiguration configuration = new VertexCentricConfiguration();
        configuration.setParallelism(1);
        Graph<Long, String, String> graph = TestedGraph.getGraph();

        // We define here a big number of iterations
        // At the end we'll see whether Gelly really computed the value in 20 iterations
        int maxIterations = 20;

        Graph<Long, String, String> result = graph.runGatherSumApplyIteration(new Gather(),
                new Sum(), new Apply(), maxIterations);

        List<Vertex<Long, String>> updatedVertices = result.getVertices().collect();
        assertThat(updatedVertices).containsAll(Arrays.asList(
                new Vertex<>(2L, "AC"), new Vertex<>(1L, ""), new Vertex<>(3L, "B"), new Vertex<>(7L, ""),
                new Vertex<>(6L, "BDF"), new Vertex<>(5L, "BDE"), new Vertex<>(4L, "BD")
        ));
        assertThat(TestedGraph.Counter.THREE_PHASES.get()).isEqualTo(3);
    }

    private static class Gather extends GatherFunction<String, String, String> {

        @Override
        public String gather(Neighbor<String, String> neighbor) {
            TestedGraph.Counter.THREE_PHASES.setExecution(getSuperstepNumber());
            return neighbor.getNeighborValue() + neighbor.getEdgeValue();
        }
    }

    private static class Sum extends SumFunction<String, String, String> {

        @Override
        public String sum(String value1, String value2) {
            // It's invoked only when there are more than 1 value returned by gather step
            return value1 + value2;
        }
    }

    private static class Apply extends ApplyFunction<Long, String, String> {

        @Override
        public void apply(String newValue, String currentValue) {
            // It's called at the end for every active vertex
            // When the value is really updated, i.e. it's different from the previous value, the updated vertex
            // remains active and its neighbors are invoked in the next superstep
            // Here, only for testing, we sort the values to get always the same results (sometimes A and C are inversed)
            this.setResult(TestedGraph.lettersToSorted(newValue));
        }
    }
}
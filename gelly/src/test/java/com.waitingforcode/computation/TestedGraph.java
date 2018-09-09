package com.waitingforcode.computation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestedGraph {

    public static Graph<Long, String, String> getGraph() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Edge<Long, String>> edges = Arrays.asList(new Edge<>(1L, 2L, "A"), new Edge<>(1L, 3L, "B"),
                new Edge<>(3L, 4L, "D"), new Edge<>(4L, 5L, "E"), new Edge<>(4L, 6L, "F"), new Edge<>(7L, 2L, "C")
        );
        List<Vertex<Long, String>> vertices = Arrays.asList(new Vertex<>(1L, ""), new Vertex<>(2L, ""),
                new Vertex<>(3L, ""), new Vertex<>(4L, ""), new Vertex<>(5L, ""), new Vertex<>(6L, ""), new Vertex<>(7L, "")
        );
        return Graph.fromCollection(vertices, edges, env);
    }

    public static String lettersToSorted(String lettersToSort) {
        List<String> letters = Arrays.asList(lettersToSort.split(""));
        letters.sort(Comparator.naturalOrder());
        return String.join("", letters);
    }

    public enum Counter {
        ONE_PHASE(new AtomicInteger(0)),
        TWO_PHASES(new AtomicInteger(0)),
        THREE_PHASES(new AtomicInteger(0));

        private AtomicInteger executions;

        Counter(AtomicInteger executions) {
            this.executions = executions;
        }

        public void setExecution(int nr) {
            executions.set(nr);
        }

        public int get() {
            return executions.get();
        }
    }

}

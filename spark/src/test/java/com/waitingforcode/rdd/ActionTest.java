package com.waitingforcode.rdd;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.waitingforcode.model.Player;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ActionTest {

    private static final int FR = 1;
    private static final int DE = 2;
    private static final int PL = 3;

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Action Test").setMaster("local[1]");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);

    @Test
    public void should_count_all_filtered_numbers() {
        List<Integer> numbers = Lists.newArrayList(1, 2, 3, 4, 5, 6, 6, 6);

        // count() doesn't distinct duplicated values
        long allDefinedNotes = CONTEXT.parallelize(numbers).count();

        assertThat(allDefinedNotes).isEqualTo(8);
    }

    @Test
    public void should_take_only_first_2_numbers() {
        List<Integer> numbers = Lists.newArrayList(1, 2, 3, 4, 5, 6);

        // count() doesn't distinct duplicated values
        List<Integer> top2Numbers = CONTEXT.parallelize(numbers).top(2);

        assertThat(top2Numbers).hasSize(2);
        assertThat(top2Numbers).containsExactly(6, 5);
    }

    @Test
    public void should_take_ordered_2_biggest_numbers() {
        List<Integer> numbers = Lists.newArrayList(10, 49, 1, 2, 30, 3, 4, 64, 5, 6);

        // count() doesn't distinct duplicated values
        List<Integer> top2Numbers = CONTEXT.parallelize(numbers).takeOrdered(2, Comparator.<Integer>naturalOrder().reversed());

        assertThat(top2Numbers).hasSize(2);
        assertThat(top2Numbers).containsExactly(64, 49);
    }

    @Test
    public void should_multiply_each_entry_by_2_without_changing_object_out_of_scope() {
        List<Integer> numbers = Lists.newArrayList(3, 6, 12, 24, 48, 96);

        // count() doesn't distinct duplicated values
        List<Integer> multipliedBy2 = new ArrayList<>();
        JavaRDD<Integer> rddNumbers = CONTEXT.parallelize(numbers);
        rddNumbers.foreach(number -> {
            multipliedBy2.add(number*2);
            System.out.println("List<Integer> multipliedBy2 after adding: "+multipliedBy2);
        });

        // Even if foreach is called locally, it won't work as forEach from Java 8.
        // In fact, Spark submits executable code as a serialized closure. All variables defined
        // and used inside these closures are the copies of real variables.
        // So in our case each executor is working on copy of List<Integer> multipliedBy2.
        // We can apperceive that in System's output print which, after the last iteration,
        // prints something like that:
        // <pre>
        // List<Integer> multipliedBy2 after adding: [6, 12, 24, 48, 96, 192]
        // </pre>
        // A solution for this error could be the use of Accumulators, as in the test called
        // should_correctly_apply_foreach_for_accumulator . It helps to collect manipulated data
        // across different executors.
        assertThat(multipliedBy2).isEmpty();
        assertThat(rddNumbers.collect()).containsExactly(3, 6, 12, 24, 48, 96);
    }

    /*@Test
    public void should_correctly_apply_foreach_for_accumulator() {
        Accumulator accumulator = new Accumulator("", new StringParam());
        List<String> numbersStr = Lists.newArrayList("3", "6", "12");
        CONTEXT.parallelize(numbersStr).foreach((String number) -> accumulator.add(number));

        assertThat(accumulator.value()).isEqualTo("3_6_12");
    } */

    @Test
    public void should_return_correct_count_of_distinct_values() {
        List<String> letters = Lists.newArrayList("A", "A", "A", "B", "B", "C");

        // Two methods exist to do that - either take(1) or first()
        Map<String, Long> counter = CONTEXT.parallelize(letters).countByValue();

        assertThat(counter).hasSize(3);
        assertThat(counter.get("A")).isEqualTo(3L);
        assertThat(counter.get("B")).isEqualTo(2L);
        assertThat(counter.get("C")).isEqualTo(1L);
    }

    @Test
    public void should_correctly_count_by_key() {
        // tuple represents different animal owner; The key corresponds to the
        // kind of animal and the value to the number of animals in its own, for example:
        // "cat", 2 -> one owner has 2 cats
        // "dog", 7 -> one owner has 7 dogs
        List<Tuple2<String, Integer>> speciesCounterList = Lists.newArrayList(
                new Tuple2<>("cat", 2), new Tuple2<>("dog", 7), new Tuple2<>("cat", 4), new Tuple2<>("duck", 11)
        );

        JavaPairRDD<String, Integer> speciesRDD = CONTEXT.parallelizePairs(speciesCounterList);

        // we want to know the number of animals owned by all owners
        // as you can see, we do not want to know the sum of all species
        Map<String, Long> ownersCounter = speciesRDD.countByKey();

        assertThat(ownersCounter.get("cat")).isEqualTo(2L);
        assertThat(ownersCounter.get("dog")).isEqualTo(1L);
        assertThat(ownersCounter.get("duck")).isEqualTo(1L);
    }

    @Test
    public void should_collect_distinct_letters() {
        List<String> letters = Lists.newArrayList("A", "A", "A", "B", "B", "C");

        List<String> distinctLetters = CONTEXT.parallelize(letters)
                .distinct()
                .collect();

        assertThat(distinctLetters).hasSize(3);
        assertThat(distinctLetters).containsOnly("A", "B", "C");
    }

    @Test
    public void should_take_the_first_defined_player() {
        Player player1 = new Player("Player1", "Team1", FR, 10, 0);
        Player player2 = new Player("Player2", "Team1", FR, 5, 3);
        Player player3 = new Player("Player3", "Team2", FR, 7, 10);
        Player player4 = new Player("Player4", "Team1", DE, 17, 10);
        Player player5 = new Player("Player5", "Team2", PL, 3, 1);
        List<Player> players = Lists.newArrayList(player1, player4,   player3, player2, player5);

        // Two methods exist to do that - either take(1) or first()
        List<Player> theFirstPlayerMethod1 = CONTEXT.parallelize(players).take(1);
        Player theFirstPlayerMethod2 = CONTEXT.parallelize(players).first();

        assertThat(theFirstPlayerMethod1).hasSize(1);
        assertThat(theFirstPlayerMethod1.get(0).getName()).isEqualTo("Player1");
        assertThat(theFirstPlayerMethod2.getName()).isEqualTo("Player1");
    }

    @Test
    public void should_return_3_the_best_scorers() {
        Player player1 = new Player("Player1", "Team1", FR, 10, 0);
        Player player2 = new Player("Player2", "Team1", FR, 5, 3);
        Player player3 = new Player("Player3", "Team2", FR, 7, 10);
        Player player4 = new Player("Player4", "Team1", DE, 17, 10);
        Player player5 = new Player("Player5", "Team2", PL, 3, 1);
        List<Player> players = Lists.newArrayList(player1, player4,   player3, player2, player5);

        // Exactly in the same fashion, with the use of min() method, we can get the worst scorer
        List<Player> theBestScorers = CONTEXT.parallelize(players)
                .top(3, new GoalsComparator());

        assertThat(theBestScorers).hasSize(3);
        assertThat(theBestScorers).extracting("name").containsExactly("Player4", "Player1", "Player3");
        assertThat(theBestScorers).extracting("goals").containsExactly(17, 10, 7);
    }

    @Test
    public void should_return_the_best_scorer() {
        Player player1 = new Player("Player1", "Team1", FR, 10, 0);
        Player player2 = new Player("Player2", "Team1", FR, 5, 3);
        Player player3 = new Player("Player3", "Team2", FR, 7, 10);
        Player player4 = new Player("Player4", "Team1", DE, 17, 10);
        Player player5 = new Player("Player5", "Team2", PL, 3, 1);
        List<Player> players = Lists.newArrayList(player1, player4,   player3, player2, player5);

        // Exactly in the same fashion, with the use of min() method, we can get the worst scorer
        Player theBestScorer = CONTEXT.parallelize(players)
                .max(new GoalsComparator());

        assertThat(theBestScorer.getName()).isEqualTo("Player4");
    }

    @Test
    public void should_sample_RDD() {
        List<String> letters = Lists.newArrayList("A","B", "C", "D");

        JavaRDD<String> allLettersRDD = CONTEXT.parallelize(letters);

        List<String> sampledLetters = allLettersRDD.takeSample(true, 2, 3L);

        assertThat(sampledLetters).hasSize(2);
        assertThat(sampledLetters).containsExactly("A", "B");
    }

    @Test
    public void should_correctly_reduce_results_to_check_if_somebody_has_a_debth() {
        List<Integer> numbers = Lists.newArrayList(-1, -2, 3);

        JavaRDD<Integer> numbersRDD = CONTEXT.parallelize(numbers, 1);

        // Reduce applies to all pair of elements held by RDD. It stops to apply reduce function when only
        // one entry is left. The type of result is the same as the type of input parameters.
        // -1 + -2 = -3; -3 + 3
        int result = numbersRDD.reduce((firstValue, secondValue) -> firstValue+secondValue);
        assertThat(result).isEqualTo(0);
    }

    @Test
    public void should_correctly_aggregate_results() {
        List<Integer> numbers = Lists.newArrayList(1, 2, 3, 4);

        JavaRDD<Integer> numbersRDD = CONTEXT.parallelize(numbers, 2);

        // Aggregates values of given RDD. The first value is called 'zero value' and it's used to begin the
        // computation. After, we can find combine function, operating on entries inside RDD's partition. The last
        // function merges results computed on different partitions.
        // Unlike fold(), it can create output of different type than input.
        // 1 + 2 + 3 + 4 = 10
        int result = numbersRDD.aggregate(0,
                (first, second) -> first+second,
                (firstPartition, secondPartition) -> firstPartition + secondPartition);
        assertThat(result).isEqualTo(10);
    }

    @Test
    public void should_correctly_use_fold_for_sum_starting_from_negative_sum_of_numbers_when_slices_are_the_same_as_items_number() {
        List<Integer> numbers = Lists.newArrayList(1, 2, 3, 4);

        JavaRDD<Integer> numbersRDD = CONTEXT.parallelize(numbers, 2);

        // Fold is also a grouping function. By taking one input type, it generates
        // the output of the same type.
        // Spark's fold is suitable for computations made in parallel way. If we expect to calculate
        // fold() on different executors, fold() is able to be called locally on each executor. Local results
        // are further combined to the final one.
        // However, it can't apply to any kind of function. Used function must be commutative. It means that
        // its parameters combinations must give the same results. For example a+b = b+a is a commutative function,
        // but a-b and b-a is not.
        int result = numbersRDD.fold(0, (previousValue, currentValue) -> {
            System.out.println("Addition: "+previousValue + " + " + currentValue);
            return currentValue+previousValue;
        });

        // 1st executor: 0+1 = 1; 1+2 = 3
        // 2nd executor: 0+3 = 3; 3+4 = 7
        // Results merge: 7+3 = 10
        assertThat(result).isEqualTo(10);
    }

    @Test
    public void should_correctly_save_numbers_in_text_file() throws IOException {
        File sparkRDDFile = new File("./test_dir");
        if (sparkRDDFile.exists()) {
            Arrays.stream(sparkRDDFile.listFiles()).forEach(f -> f.delete());
            sparkRDDFile.delete();
        }
        List<Integer> numbers = Lists.newArrayList(1, 2, 3, 4);

        JavaRDD<Integer> numbersRDD = CONTEXT.parallelize(numbers, 2);

        numbersRDD.saveAsTextFile("./test_dir");

        sparkRDDFile = new File("./test_dir");
        assertThat(sparkRDDFile.exists()).isTrue();
        File part1 = new File("./test_dir/part-00000");
        File part2 = new File("./test_dir/part-00001");
        assertThat(Files.readLines(part1, Charset.defaultCharset()).stream().collect(Collectors.joining(", "))).isEqualTo("1, 2");
        assertThat(Files.readLines(part2, Charset.defaultCharset()).stream().collect(Collectors.joining(", "))).isEqualTo("3, 4");
        // Saving output as a file generated N number of content files, N  number of checksum files, where
        // N is the number of partitions (2 in our case). Content files hold data triggered by action. They're prefixed by
        // part-XXXXX. Each of them is accompanied by Cyclic Redudancy Checks file (.crc) which checks if stored data
        // is correct.
        // After, ther are also _SUCCESS and _SUCCESS.crc files. They exist simply to tell that operation of saving
        // data into file terminated correctly.
        // Sample output for our example looks like:
        // > part-00001: [3, 4]
        // > ._SUCCESS.crc: [crc    ]
        // > .part-00001.crc: [crc    ��l]
        // > _SUCCESS: []
        // > .part-00000.crc: [crc    i�xa]
        // > part-00000: [1, 2]
    }

    // Comparator must be serializable too. Otherwise the task will fail with the exception
    // telling that the Task is not serializable
    // More about the reason of this error:
    // https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/javaionotserializableexception.html
    // See also comment on Player object
    private static class GoalsComparator implements Comparator<Player>, Serializable {

        @Override
        public int compare(Player player1, Player player2) {
            if (player1.getGoals() == player2.getGoals()) {
                return 0;
            }
            return player1.getGoals() > player2.getGoals() ? 1 : -1;
        }
    }

    private static class StringParam implements  AccumulatorParam<String>, Serializable {
        @Override
        public String addAccumulator(String t1, String t2) {
            return t1 + "_" + t2;
        }

        @Override
        public String addInPlace(String r1, String r2) {
            System.out.println("addInPlace " + r1 + " and " + r2);
            return r1+r2;
        }

        @Override
        public String zero(String initialValue) {
            return initialValue;
        }
    }

}

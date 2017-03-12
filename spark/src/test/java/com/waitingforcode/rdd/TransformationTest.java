package com.waitingforcode.rdd;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.waitingforcode.model.NotSerializablePlayer;
import com.waitingforcode.model.Player;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TransformationTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Transformation Test").setMaster("local[1]");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONFIGURATION);
    private static final int FR = 1;
    private static final int DE = 2;
    private static final int PL = 3;

    @Test
    public void should_retain_players_having_at_least_20_points() {
        Player player1 = new Player("Player1", "Team1", FR, 10, 0);
        Player player2 = new Player("Player2", "Team1", FR, 5, 3);
        Player player3 = new Player("Player3", "Team1", FR, 7, 10);
        List<Player> players = Lists.newArrayList(player1, player2, player3);

        JavaRDD<Player> efficientPlayersRDD = CONTEXT.parallelize(players)
                .filter(player -> player.getPoints() >= 20);

        Collection<Player> efficientPlayers = efficientPlayersRDD.collect();

        assertThat(efficientPlayers).hasSize(2);
        assertThat(efficientPlayers).extracting("name").containsOnly("Player1", "Player3");
    }

    @Test
    public void should_sort_players_by_team_and_name() {
        Player player1 = new Player("Player1", "Team1", FR, 10, 0);
        Player player2 = new Player("Player2", "Team1", FR, 5, 3);
        Player player3 = new Player("Player3", "Team2", FR, 7, 10);
        Player player4 = new Player("Player4", "Team1", DE, 17, 10);
        Player player5 = new Player("Player5", "Team2", PL, 3, 1);
        List<Player> players = Lists.newArrayList(player1, player4,   player3, player2, player5);

        JavaRDD<Player> playersRDD = CONTEXT.parallelize(players)
                .sortBy(player -> player.getTeam()+"_"+player.getName(), true, 1);

        List<Player> sortedPlayers = playersRDD.collect();

        assertThat(sortedPlayers).hasSize(5);
        assertThat(sortedPlayers).extracting("team").containsExactly("Team1", "Team1", "Team1", "Team2", "Team2");
        assertThat(sortedPlayers).extracting("name").containsExactly("Player1", "Player2", "Player4", "Player3", "Player5");
    }

    @Test
    public void should_group_players_by_teams() {
        Player player1 = new Player("Player1", "Team1", FR, 10, 0);
        Player player2 = new Player("Player2", "Team1", FR, 5, 3);
        Player player3 = new Player("Player3", "Team2", FR, 7, 10);
        Player player4 = new Player("Player4", "Team1", DE, 17, 10);
        Player player5 = new Player("Player5", "Team2", PL, 3, 1);
        List<Player> players = Lists.newArrayList(player1, player2, player3, player4, player5);

        JavaPairRDD<String, Iterable<Player>> teamPlayersRDD = CONTEXT.parallelize(players)
                .groupBy(player -> player.getTeam());

        Map<String, Iterable<Player>> playersByTeam = teamPlayersRDD.collectAsMap();

        assertThat(playersByTeam).hasSize(2);
        assertThat(playersByTeam).containsOnlyKeys("Team1", "Team2");
        assertThat(playersByTeam.get("Team1").iterator()).extracting("name").containsOnly("Player1", "Player2", "Player4");
        assertThat(playersByTeam.get("Team2").iterator()).extracting("name").containsOnly("Player3", "Player5");
    }

    @Test
    public void should_correctly_map_players_for_teams() {
        Player player1 = new Player("Player1", "Team1", FR, 10, 0);
        Player player2 = new Player("Player2", "Team1", FR, 5, 3);
        Player player3 = new Player("Player3", "Team2", FR, 7, 10);
        Player player4 = new Player("Player4", "Team1", DE, 17, 10);
        Player player5 = new Player("Player5", "Team2", PL, 3, 1);
        List<Player> players = Lists.newArrayList(player1, player4,   player3, player2, player5);

        // map() is different than groupBy() because map() makes a kind of
        // object transformation while the groupBy() takes already existent
        // objects and put them under a common "address"
        JavaRDD<String> playerNamesRDD = CONTEXT.parallelize(players)
                .map(player -> player.getName());

        List<String> playerNames = playerNamesRDD.collect();

        assertThat(playerNames).hasSize(5);
        assertThat(playerNames).containsOnly("Player1", "Player2", "Player3", "Player4", "Player5");
    }

    @Test
    public void should_correctly_flat_a_map() {
        List<String> animals = Arrays.asList("cat", "dog", "fish", "chicken");
        JavaRDD<Integer> flattenKeywordsRDD = CONTEXT.parallelize(animals)
                // in 1.6.2 was Arrays.asList(...), since 2.0.0, it returns an iterator: http://stackoverflow.com/questions/38880956/spark-2-0-0-arrays-aslist-not-working-incompatible-types
                .flatMap(animalName -> Arrays.asList(animalName.length()).iterator());
        JavaRDD<List<Integer>> notFlattenKeywordsRDD = CONTEXT.parallelize(animals)
                .map(animalName -> Arrays.asList(animalName.length()));
        List<Integer> flattenLengths = flattenKeywordsRDD.collect();
        List<List<Integer>> notFlattenLengths = notFlattenKeywordsRDD.collect();

        // The difference between map() and flattenMap() is visible thanks to the type
        // returned by collect() method
        // In the case of map(), we apply a function to each entry of List<String> keywords object
        // and the returned type applies for each of entries
        // In the other side, flatMap() applies a function to each entry of List<String> keywords object
        // but the final output is a concatenation of all separate ones
        assertThat(notFlattenLengths).hasSize(4);
        assertThat(notFlattenLengths).contains(Arrays.asList(3), Arrays.asList(3),  Arrays.asList(4), Arrays.asList(7));
        assertThat(flattenLengths).hasSize(4);
        assertThat(flattenLengths).containsOnly(3, 3, 4, 7);
    }

    @Test
    public void should_return_distinct_letters() {
        List<String> letters = Lists.newArrayList("A", "B", "C", "D", "E", "E", "E", "E");

        JavaRDD<String> lettersRDD = CONTEXT.parallelize(letters).distinct();

        List<String> distinctLetters = lettersRDD.collect();

        assertThat(distinctLetters).hasSize(5);
        assertThat(distinctLetters).containsOnly("A", "B", "C", "D", "E");
    }

    @Test
    public void should_combine_two_different_RDDs() {
        List<String> letters1 = Lists.newArrayList("A", "E", "O", "I", "A", "A", "E");
        List<String> letters2 = Lists.newArrayList("D", "K", "K", "L", "D", "L", "K", "N", "D");

        JavaRDD<String> letters1RDD = CONTEXT.parallelize(letters1);
        JavaRDD<String> letters2RDD = CONTEXT.parallelize(letters2);
        JavaRDD<String> allLettersRDD = letters1RDD.union(letters2RDD).distinct();

        List<String> allLetters = allLettersRDD.collect();

        assertThat(allLetters).hasSize(8);
        assertThat(allLetters).containsOnly("A", "E", "O", "I", "D", "K", "L", "N");
    }

    @Test
    public void should_correctly_subtract_letters() {
        List<String> letters1 = Lists.newArrayList("A", "B", "C", "D", "E", "F", "F");
        List<String> letters2 = Lists.newArrayList("B", "C", "D", "F");

        // subtract() transformation returns a new RDD having elements of 'current' RDD which
        // are not defined in the 'other' RDD
        // 'current' means here the RDD on which we apply the transformation
        // 'other' is the RDD applying in transformation
        // The subtract is not made 1 by 1. It's the reason why we don't see
        // "F" in subtracted RDD, even if it's defined twice in 'current' RDD
        // and only once in 'other' RDD
        JavaRDD<String> letters1RDD = CONTEXT.parallelize(letters1);
        JavaRDD<String> letters2RDD = CONTEXT.parallelize(letters2);
        JavaRDD<String> subtractedRDD = letters1RDD.subtract(letters2RDD);

        List<String> collectAfterSubtract = subtractedRDD.collect();

        assertThat(collectAfterSubtract).hasSize(2);
        assertThat(collectAfterSubtract).containsOnly("A", "E");
    }

    @Test
    public void should_correctly_compute_cartesian_product() {
        List<String> vowels = Lists.newArrayList("A","E");
        List<String> consonants = Lists.newArrayList("B", "C", "D", "F");

        JavaRDD<String> vowelsRDD = CONTEXT.parallelize(vowels);
        JavaRDD<String> consonantsRDD = CONTEXT.parallelize(consonants);
        JavaPairRDD<String, String> cartesianLettersRDD = vowelsRDD.cartesian(consonantsRDD);

        List<Tuple2<String, String>> cartesianLetters = cartesianLettersRDD.collect();
        List<String> normalizedCartesianLetters =
                cartesianLetters.stream().map(tuple -> tuple._1()+"_"+tuple._2()).collect(Collectors.toList());

        assertThat(normalizedCartesianLetters).hasSize(8);
        assertThat(normalizedCartesianLetters).containsOnly("A_B", "A_C", "A_D", "A_F", "E_B", "E_C", "E_D", "E_F");
    }

    @Test
    public void should_correctly_apply_intersection_transformation() {
        List<String> letters1 = Lists.newArrayList("A","E", "D", "F", "X", "Y", "Z", "D", "F");
        List<String> letters2 = Lists.newArrayList("B", "C", "D", "F");

        // intersection() simply returns common elements in both RDD
        // Returned elements aren't duplicated. So even if the first RDD contains
        // "D" and "F" defined twice, they will be returned only once in the
        // intersected RDD
        JavaRDD<String> letters1RDD = CONTEXT.parallelize(letters1);
        JavaRDD<String> letters2RDD = CONTEXT.parallelize(letters2);
        JavaRDD<String> intersectionRDD = letters1RDD.intersection(letters2RDD);

        List<String> commonLetters = intersectionRDD.collect();

        assertThat(commonLetters).hasSize(2);
        assertThat(commonLetters).containsOnly("D", "F");
    }

    @Test
    public void should_sample_RDD() {
        List<String> letters = Lists.newArrayList("A","E", "A", "E");

        JavaRDD<String> allLettersRDD = CONTEXT.parallelize(letters);

        // Sampling is made either through Bernoulli sampling method or through
        // Poisson distribution. Used method depends on boolean flag set
        // as the first parameter of the method.
        // Since we specify a seed, we expect to get the same results on each test
        JavaRDD<String> sampledRDD = allLettersRDD.sample(true, 0.33, 2);

        List<String> sampledLetters = sampledRDD.collect();

        assertThat(sampledLetters).hasSize(5);
        assertThat(sampledLetters).containsExactly("A", "E", "A", "A", "E");
    }

    @Test
    public void should_correctly_apply_coalesce_transformation() {
        List<Integer> numbers = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // coalesce simply reduce data to the number of partitions defined
        // as method parameter
        // If the number of partitions decreases, whole data won't be moved
        // across network. Spark will keep the number of specified partitions
        // and move data remained on the rest of partitions to them.
        JavaRDD<Integer> sampledRDD = CONTEXT.parallelize(numbers, 2);
        JavaRDD<Integer> coalescedRDD = sampledRDD.coalesce(1);

        assertThat(sampledRDD.partitions()).hasSize(2);
        assertThat(coalescedRDD.partitions()).hasSize(1);
    }

    @Test(expected = SparkException.class)
    public void should_fail_on_transforming_not_serializable_object() {
        List<NotSerializablePlayer> players = Lists.newArrayList(
                new NotSerializablePlayer("Player1", "Team", 0, 0, 0),
                new NotSerializablePlayer("Player2", "Team", 0, 0, 0)
        );

        JavaRDD<NotSerializablePlayer> sampledRDD = CONTEXT.parallelize(players);
        JavaRDD<NotSerializablePlayer> onlyScorersRDD = sampledRDD.filter(player -> player.getGoals() > 0);
        onlyScorersRDD.collect();

    }

    // JavaPairRDD
    @Test
    public void should_correctly_join_RDDs() {
        List<Integer> numbers1 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<Integer> numbers2 = Lists.newArrayList(20, 19, 18, 17, 16, 15);

        JavaPairRDD<Integer, Iterable<Integer>> numbers1RDD = CONTEXT.parallelize(numbers1, 2).groupBy(number -> number%2);
        JavaPairRDD<Integer, Iterable<Integer>> numbers2RDD = CONTEXT.parallelize(numbers2, 2).groupBy(number -> number%2);

        // join() groups entries for two RDDs by key
        Map<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> joinedRDDMap =
                numbers1RDD.join(numbers2RDD).collectAsMap();

        assertThat(joinedRDDMap).hasSize(2);
        assertThat(joinedRDDMap.get(0)._1()).containsOnly(2, 4, 6, 8, 10);
        assertThat(joinedRDDMap.get(0)._2()).containsOnly(16, 18, 20);
        assertThat(joinedRDDMap.get(1)._1()).containsOnly(1, 3, 5, 7, 9);
        assertThat(joinedRDDMap.get(1)._2()).containsOnly(15, 17, 19);

        List<Tuple2<Integer, String>> tuples1 = Lists.newArrayList(
                new Tuple2<>(1, "a"), new Tuple2<>(2, "b"), new Tuple2<>(3, "c"), new Tuple2<>(4, "d")
        );
        List<Tuple2<Integer, String>> tuples2 = Lists.newArrayList(
                new Tuple2<>(1, "a"), new Tuple2<>(3, "c"), new Tuple2<>(3, "c"), new Tuple2<>(5, "e")
        );
        CONTEXT.parallelize(tuples1);
        JavaPairRDD<Integer, String> numbers3RDD = CONTEXT.parallelizePairs(tuples1);
        JavaPairRDD<Integer, String> numbers4RDD = CONTEXT.parallelizePairs(tuples2);

        System.out.println("> " + numbers3RDD.join(numbers4RDD).collectAsMap());
    }

    @Test
    public void should_produce_empty_join_when_the_second_RDD_doesn_t_contain_correspondances() {
        List<Integer> numbers1 = Lists.newArrayList(2, 4);
        List<Integer> numbers2 = Lists.newArrayList(19);

        JavaPairRDD<Integer, Iterable<Integer>> numbers1RDD = CONTEXT.parallelize(numbers1, 2).groupBy(number -> number%2);
        JavaPairRDD<Integer, Iterable<Integer>> numbers2RDD = CONTEXT.parallelize(numbers2, 2).groupBy(number -> number%2);

        Map<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> joinedRDDMap =
                numbers1RDD.join(numbers2RDD).collectAsMap();

        assertThat(joinedRDDMap).isEmpty();
    }

    @Test
    public void should_correctly_apply_left_outer_join() {
        List<Integer> numbers1 = Lists.newArrayList(2, 4, 6, 8, 10);
        List<Integer> numbers2 = Lists.newArrayList(3, 5, 7);

        JavaPairRDD<Integer, Iterable<Integer>> numbers1RDD = CONTEXT.parallelize(numbers1, 2).groupBy(number -> number%2);
        JavaPairRDD<Integer, Iterable<Integer>> numbers2RDD = CONTEXT.parallelize(numbers2, 2).groupBy(number -> number%2);

        // join() groups entries for two RDDs by key
        Map<Integer, Tuple2<Iterable<Integer>, org.apache.spark.api.java.Optional<Iterable<Integer>>>> joinedRDDMap =
                numbers1RDD.leftOuterJoin(numbers2RDD).collectAsMap();

        assertThat(joinedRDDMap).hasSize(1);
        assertThat(joinedRDDMap.get(0)._1()).containsOnly(2, 4, 6, 8, 10);
        assertThat(joinedRDDMap.get(0)._2().isPresent()).isFalse();
    }

    @Test
    public void should_correctly_apply_right_outer_join() {
        List<Integer> numbers1 = Lists.newArrayList(2, 4, 6, 8, 10);
        List<Integer> numbers2 = Lists.newArrayList(3, 5, 7);

        JavaPairRDD<Integer, Iterable<Integer>> numbers1RDD = CONTEXT.parallelize(numbers1, 2).groupBy(number -> number%2);
        JavaPairRDD<Integer, Iterable<Integer>> numbers2RDD = CONTEXT.parallelize(numbers2, 2).groupBy(number -> number%2);

        Map<Integer, Tuple2<org.apache.spark.api.java.Optional<Iterable<Integer>>, Iterable<Integer>>> joinedRDDMap =
                numbers1RDD.rightOuterJoin(numbers2RDD).collectAsMap();

        assertThat(joinedRDDMap).hasSize(1);
        assertThat(joinedRDDMap.get(1)._1().isPresent()).isFalse();
        assertThat(joinedRDDMap.get(1)._2()).containsOnly(3, 5, 7);
        Iterable<Integer> entries1 = Iterables.concat(joinedRDDMap.get(1)._1().or(new ArrayList<>()), joinedRDDMap.get(1)._2());
        assertThat(entries1).hasSize(3);
        assertThat(entries1).containsOnly(3, 5, 7);
    }

    @Test
    public void should_correctly_apply_full_outer_join() {
        List<Integer> numbers1 = Lists.newArrayList(2, 4, 6, 8, 10);
        List<Integer> numbers2 = Lists.newArrayList(3, 5, 7);

        JavaPairRDD<Integer, Iterable<Integer>> numbers1RDD = CONTEXT.parallelize(numbers1, 2).groupBy(number -> number%2);
        JavaPairRDD<Integer, Iterable<Integer>> numbers2RDD = CONTEXT.parallelize(numbers2, 2).groupBy(number -> number%2);

        Map<Integer, Tuple2<org.apache.spark.api.java.Optional<Iterable<Integer>>, org.apache.spark.api.java.Optional<Iterable<Integer>>>> joinedRDDMap =
                numbers1RDD.fullOuterJoin(numbers2RDD).collectAsMap();

        assertThat(joinedRDDMap).hasSize(2);
        assertThat(joinedRDDMap.get(0)._1().get()).containsOnly(2, 4, 6, 8, 10);
        assertThat(joinedRDDMap.get(0)._2().isPresent()).isFalse();
        assertThat(joinedRDDMap.get(1)._1().isPresent()).isFalse();
        assertThat(joinedRDDMap.get(1)._2().get()).containsOnly(3, 5, 7);
    }

    @Test
    public void should_correctly_reduce_values_by_key() {
        List<Tuple2<String, Integer>> owner1Animals = Lists.newArrayList(
                new Tuple2<>("cat", 2), new Tuple2<>("dog", 7), new Tuple2<>("duck", 11)
        );
        List<Tuple2<String, Integer>> owner2Animals = Lists.newArrayList(
                new Tuple2<>("cat", 2), new Tuple2<>("dog", 7), new Tuple2<>("rabbit", 1)
        );

        JavaPairRDD<String, Integer> owner1AnimalsRDD = CONTEXT.parallelizePairs(owner1Animals);
        JavaPairRDD<String, Integer> owner2AnimalsRDD = CONTEXT.parallelizePairs(owner2Animals);

        JavaPairRDD<String, Integer> subtractedAnimalsRDD = owner1AnimalsRDD.subtractByKey(owner2AnimalsRDD);

        Map<String, Integer> subtractedAnimalsMap = subtractedAnimalsRDD.collectAsMap();

        assertThat(subtractedAnimalsMap).hasSize(1);
        assertThat(subtractedAnimalsMap.get("duck")).isEqualTo(11);
    }

    @Test
    public void should_correctly_group_data_by_the_key() {
        List<Tuple2<String, Integer>> owner1Animals = Lists.newArrayList(
                new Tuple2<>("cat", 2), new Tuple2<>("dog", 7), new Tuple2<>("duck", 11)
        );
        List<Tuple2<String, Integer>> owner2Animals = Lists.newArrayList(
                new Tuple2<>("cat", 3), new Tuple2<>("dog", 4), new Tuple2<>("rabbit", 1)
        );

        JavaPairRDD<String, Integer> owner1AnimalsRDD = CONTEXT.parallelizePairs(owner1Animals);
        JavaPairRDD<String, Integer> owner2AnimalsRDD = CONTEXT.parallelizePairs(owner2Animals);

        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> groupedAnimalsRDD =
                owner1AnimalsRDD.cogroup(owner2AnimalsRDD);

        Map<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> resultMap = groupedAnimalsRDD.collectAsMap();

        assertThat(resultMap).hasSize(4);
        assertThat(resultMap).containsOnlyKeys("cat", "dog", "rabbit", "duck");
        assertThat(resultMap.get("cat")._1()).containsOnly(2);
        assertThat(resultMap.get("cat")._2()).containsOnly(3);
        assertThat(resultMap.get("dog")._1()).containsOnly(7);
        assertThat(resultMap.get("dog")._2()).containsOnly(4);
        assertThat(resultMap.get("rabbit")._1()).isEmpty();
        assertThat(resultMap.get("rabbit")._2()).containsOnly(1);
        assertThat(resultMap.get("duck")._1()).containsOnly(11);
        assertThat(resultMap.get("duck")._2()).isEmpty();
    }

    @Test
    public void should_correctly_count_values_by_key() {
        List<Integer> numbers = Lists.newArrayList(2, 4, 6, 8, 10, 65, 52, 26, 11, 15, 25, 35, 45, 55);

        JavaPairRDD<Boolean, Integer> numbersDivisibleBy2RDD =
                CONTEXT.parallelize(numbers).mapToPair(n -> new Tuple2<>(n%2 == 0, n));

        JavaPairRDD<Boolean, Integer> theBiggestNumbers =
                numbersDivisibleBy2RDD.reduceByKey((value1, value2) -> value1 > value2 ? value1 : value2);

        Map<Boolean, Integer> theBiggestNumbersMap = theBiggestNumbers.collectAsMap();
        assertThat(theBiggestNumbersMap.get(true)).isEqualTo(52);
        assertThat(theBiggestNumbersMap.get(false)).isEqualTo(65);
    }

    @Test
    public void should_get_values_fo_one_rdd() {
        List<Integer> numbers = Lists.newArrayList(2, 4, 6, 8, 10, 65, 52, 26, 11, 15, 25, 35, 45, 55, 55);

        JavaPairRDD<Boolean, Integer> numbersDivisibleBy2RDD =
                CONTEXT.parallelize(numbers).mapToPair(n -> new Tuple2<>(n%2 == 0, n));

        JavaRDD<Integer> allNumbersRDD = numbersDivisibleBy2RDD.values();

        List<Integer> allNumbers = allNumbersRDD.collect();
        assertThat(allNumbers).hasSize(numbers.size());
        assertThat(allNumbers).containsAll(numbers);
    }

    @Test
    public void should_get_keys_fo_one_rdd() {
        List<Integer> numbers = Lists.newArrayList(2, 4, 6, 8, 10, 65, 52, 26, 11, 15, 25, 35, 45, 55);

        JavaPairRDD<Boolean, Integer> numbersDivisibleBy2RDD =
                CONTEXT.parallelize(numbers).mapToPair(n -> new Tuple2<>(n%2 == 0, n));

        JavaRDD<Boolean> allKeysRDD = numbersDivisibleBy2RDD.keys();

        List<Boolean> allKeys = allKeysRDD.collect();
        // keys can be duplicated
        assertThat(allKeys).hasSize(14);
        long trueValues = allKeys.stream().filter(value -> value).count();
        long falseValues = allKeys.stream().filter(value -> !value).count();
        assertThat(trueValues).isEqualTo(7);
        assertThat(falseValues).isEqualTo(7);
    }

    @Test
    public void should_correctly_group_values_by_key() {
        List<Tuple2<Boolean, Integer>> primeNumbers = Lists.newArrayList(
                new Tuple2<>(true, 2), new Tuple2<>(true, 7), new Tuple2<>(false, 4), new Tuple2<>(true, 11)
        );

        JavaPairRDD<Boolean, Integer> primeNumbersRDD = CONTEXT.parallelizePairs(primeNumbers);

        JavaPairRDD<Boolean, Iterable<Integer>> groupedPrimeNumbersRDD = primeNumbersRDD.groupByKey();

        Map<Boolean, Iterable<Integer>> groupedRDD = groupedPrimeNumbersRDD.collectAsMap();
        assertThat(groupedRDD).hasSize(2);
        assertThat(groupedRDD.get(true)).containsOnly(2, 7, 11);
        assertThat(groupedRDD.get(false)).containsOnly(4);
    }

    @Test
    public void should_correctly_aggregate_values_by_key() {
        List<Tuple2<Boolean, Integer>> primeNumbers = Lists.newArrayList(
                new Tuple2<>(true, 2), new Tuple2<>(true, 7), new Tuple2<>(false, 4), new Tuple2<>(true, 11)
        );

        // aggregateByKey aggregates values on each key. The first argument is the "zero" value,
        // the second one corresponds to make an operation on data held by RDD on 1 partition.
        // The last method is a combine function which merges data generated by the previous method on different
        // partitions.
        // Note that the third function is applied only when data is stored in 2 or more partitions and
        // only if at least 2 partitions aggregated results for given key.
        // Aggregate's advantage regarding to grouping by key is that it can return
        // different type than the one stored initially in transformed RDD.
        JavaPairRDD<Boolean, Integer> primeNumbersRDD = CONTEXT.parallelizePairs(primeNumbers, 2);
        JavaPairRDD<Boolean, String> aggregatedToPrintFormatRDD = primeNumbersRDD.aggregateByKey("",
                (String previousValue, Integer currentValue) -> {
                    if (previousValue.equals("")) {
                        return "["+currentValue+"]";
                    }
                    return previousValue+", ["+currentValue+"]";
                },
                (String v1, String v2) -> v1 + ", " + v2);

        Map<Boolean, String> mappedPrimeNumbers = aggregatedToPrintFormatRDD.collectAsMap();
        assertThat(mappedPrimeNumbers).hasSize(2);
        assertThat(mappedPrimeNumbers.get(false)).isEqualTo("[4]");
        assertThat(mappedPrimeNumbers.get(true)).isEqualTo("[2], [7], [11]");
    }

    @Test
    public void should_correctly_partition_data() throws InterruptedException {
        List<Integer> numbers = Lists.newArrayList(2, 4, 6, 8, 10, 65, 52, 26, 11, 15, 25, 35, 45, 55);

        JavaPairRDD<Boolean, Integer> numbersDivisibleBy2RDD =
                CONTEXT.parallelize(numbers).mapToPair(n -> new Tuple2<>(n%2 == 0, n));

        PartitionerTest partitioner = new PartitionerTest();
        JavaPairRDD<Boolean, Integer> partitionedRDD = numbersDivisibleBy2RDD.partitionBy(partitioner);

        partitionedRDD.count();
        assertThat(partitionedRDD.partitions()).hasSize(2);
        assertThat(partitionedRDD.partitions().get(0).index()).isEqualTo(0);
        assertThat(partitionedRDD.partitions().get(1).index()).isEqualTo(1);
    }

    @Test
    public void should_correctly_sort_data_by_key() {
        List<Tuple2<Integer, String>> goalsAndPlayers = Arrays.asList(
                new Tuple2<>(30, "Player1"),
                new Tuple2<>(21, "Player2"),
                new Tuple2<>(35, "Player3"),
                new Tuple2<>(15, "Player4")
        );
        JavaPairRDD<Integer, String> goalsAndPlayersRDD =
                CONTEXT.parallelizePairs(goalsAndPlayers);

        JavaPairRDD<Integer, String> sortedGoalsAndPlayersRDD = goalsAndPlayersRDD.sortByKey(false);

        // Note that sorted results are collected only on collect or save call. For example, if you call
        // collectAsMap(), the output won't be sorted as expected
        List<Tuple2<Integer, String>> goalsAndPlayersMap = sortedGoalsAndPlayersRDD.collect();
        assertThat(goalsAndPlayersMap).hasSize(4);
        List<String> sorted = new ArrayList<>();
        goalsAndPlayersMap.forEach((t) -> sorted.add(t._1()+"_"+t._2()));
        assertThat(sorted).containsExactly("35_Player3", "30_Player1", "21_Player2", "15_Player4");
    }

    @Test
    public void should_correctly_combine_by_key() {
        List<Integer> numbers = Lists.newArrayList(2, 4, 6, 8, 10, 65, 52, 26, 11, 15, 25, 35, 45, 55);

        JavaPairRDD<Boolean, Integer> numbersDivisibleBy2RDD =
                CONTEXT.parallelize(numbers, 2).mapToPair(n -> new Tuple2<>(n%2 == 0, n));

        // combine is similar operation to previously discovered (aggregation, reduce). It takes values
        // identified by keys and applies on them, first, combiner function. It can, as in our test,
        // change the type of original object from RDD to another type. The second function feeds element
        // crated by the previous function. The last function is applied when data is stored on
        // different partitions and we need to merge them.
        // Note that combineByKey works almost like groupByKey with some subtle and important
        // differences. combineByKey will combine data locally, on given partition and
        // shuffle across the network only the result of this operation. In the other side,
        // groupyBy will send not grouped entries to one specific partition where grouping
        // will occur. By the way, it's the reason why merge combiner (the 3rd) function
        // exists.
        JavaPairRDD<Boolean, List<String>> combinedRDD = numbersDivisibleBy2RDD.combineByKey(
                number -> Lists.newArrayList(number.toString()),
                (List<String> numbersList, Integer number) -> {
                    numbersList.add(number.toString());
                    return numbersList;
                },
                (List<String> partition1Data, List<String> partition2Data) -> {
                    partition1Data.addAll(partition2Data);
                    return partition1Data;
                }
            );

        Map<Boolean, List<String>> result = combinedRDD.collectAsMap();

        assertThat(result).hasSize(2);
        assertThat(result.get(true)).containsOnly("2","4","6","8","10","52","26");
        assertThat(result.get(false)).containsOnly("65","11","15","25","35","45","55");
    }

    private static class PartitionerTest extends Partitioner implements Serializable {

        @Override
        public int numPartitions() {
            return 2;
        }

        @Override
        public int getPartition(Object key) {
            // True goes to the 1st partition, False to the 2nd
            return (boolean) key ? 0 : 1;
        }
    }

}
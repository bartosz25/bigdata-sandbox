package com.waitingforcode.rdd;

import org.apache.spark.util.SizeEstimator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectsSizeTest {

    @Test
    public void should_correctly_estimate_long_size() {
        Long age = new Long(30);

        assertThat(SizeEstimator.estimate(age)).isEqualTo(24L);
    }

    @Test
    public void should_estimate_string_object_size() {
        String letter = "a";

        assertThat(SizeEstimator.estimate(letter)).isEqualTo(48L);
    }

    @Test
    public void should_correctly_estimate_bean_size() {
        Person person = new Person("a", 30L);

        assertThat(SizeEstimator.estimate(person)).isEqualTo(72L);
    }

    private static class Person {
        private long age;
        private String letter;

        private Person(String letter, long age) {
            this.letter = letter;
            this.age = age;
        }

    }


}

package com.waitingforcode.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class EncoderTest {

    private static final SparkSession SESSION = SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "/tmp/spark")
            .appName("Encoder Test").getOrCreate();

    @Test
    public void should_c() {
        DatasetTest.City london = DatasetTest.City.valueOf("London", "the United Kingdom", "Europe");

        Encoder<DatasetTest.City> cityEncoder = Encoders.bean(DatasetTest.City.class);
        Dataset<DatasetTest.City> dataset = SESSION.createDataset(Arrays.asList(london), cityEncoder)
                .filter("name != ''");

        assertThat(dataset.count()).isEqualTo(1);
    }

    @Test
    public void should_() {
        Person person = new Person("Login_Login_Login_Login", "London", 30);
        Encoder<Person> cityEncoder = Encoders.bean(Person.class);
        Dataset<Person> dataset = SESSION.createDataset(Arrays.asList(person), cityEncoder)
                .filter("login != ''");

        assertThat(dataset.count()).isEqualTo(1);
    }

    @Test
    public void should_fixed() {
        Numbers number1 = new Numbers();
        number1.setN1(200);
        number1.setN2(333);
        Encoder<Numbers> cityEncoder = Encoders.bean(Numbers.class);
        Dataset<Numbers> dataset = SESSION.createDataset(Arrays.asList(number1), cityEncoder)
                .filter("n1 != ''");

        assertThat(dataset.count()).isEqualTo(1);
    }

    public static class Numbers {
        private int n1;

        private int n2;


        public int getN1() {
            return n1;
        }

        public void setN1(int n1) {
            this.n1 = n1;
        }

        public int getN2() {
            return n2;
        }

        public void setN2(int n2) {
            this.n2 = n2;
        }
    }

    public static class Person {

        private String login;

        private String city;

        private int age;

        public Person(String login, String city, int age) {
            this.login = login;
            this.city = city;
            this.age = age;
        }

        public void setLogin(String login) {
            this.login = login;
        }

        public String getLogin() {
            return login;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getCity() {
            return city;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public int getAge() {
            return age;
        }
    }

}

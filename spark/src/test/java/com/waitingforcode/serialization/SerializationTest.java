package com.waitingforcode.serialization;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

public class SerializationTest {

    private static final SparkConf CONFIGURATION =
            new SparkConf().setAppName("Serialization Test").setMaster("local[1]")
                    .set("spark.driver.allowMultipleContexts", "true")
                    .set("spark.ui.enabled", "false");

    @Test
    public void should_fail_on_serializing_class_not_implementing_serializable() {
        JavaSparkContext sparContext = getJavaSerializationContext();
        JavaRDD<PersonNotSerializable> personRDD =
                sparContext.parallelize(Collections.singletonList(new PersonNotSerializable()));
        try {
            personRDD.count();
            fail("NotSerializableException should be thrown when manipulated" +
                    " object doesn't implement Serializable");
        } catch (Exception se) {
            assertThat(se.getMessage())
                    .contains("object not serializable (class: com.waitingforcode.serialization.SerializationTest$PersonNotSerializable,");
        }
    }

    @Test
    public void should_fail_on_serializing_not_registered_class() {
        Class[] registeredClasses = new Class[]{Person.class, scala.collection.mutable.WrappedArray.ofRef.class, Object[].class};
        JavaSparkContext sparkContext =
                getKryoSerializationContext(registeredClasses, new Tuple2<>("spark.kryo.registrationRequired", "true"));
        List<Person> personData = Collections.singletonList(Person.valueOf("Name", 30, Address.valueOf("street", "city")));
        JavaRDD<Person> personRDD = sparkContext.parallelize(personData);

        try {
            personRDD.count();
            fail("Should fail because Person class is not registered");
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("Class is not registered:" +
                    " com.waitingforcode.serialization.SerializationTest$Address");
        }
    }

    @Test
    public void should_correctly_serialize_registered_class() {
        Class[] registeredClasses = new Class[]{Person.class, Address.class,
                scala.collection.mutable.WrappedArray.ofRef.class, Object[].class};
        JavaSparkContext sparkContext =
                getKryoSerializationContext(registeredClasses, new Tuple2<>("spark.kryo.registrationRequired", "true"));
        List<Person> personData = Collections.singletonList(Person.valueOf("Name", 30, Address.valueOf("street", "city")));
        JavaRDD<Person> personRDD = sparkContext.parallelize(personData);

        long notNullCounter = personRDD.filter(person -> person != null).count();

        assertThat(notNullCounter).isEqualTo(1);
    }

    @Test
    public void should_compare_size_of_registered_unregistered_and_java_serialized_objects() throws IOException, InterruptedException {
        String checkpointDir = "/tmp/spark/checkpoints";
        FileUtils.cleanDirectory(new File(checkpointDir));
        // serialized data
        List<Person> personData = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            personData.add(Person.valueOf("Name"+i, 30+i, Address.valueOf("street"+i, "city"+i)));
        }
        // First, we check Kroy with registered classes
        Class[] registeredClasses = new Class[]{Person.class, Address.class,
                scala.collection.mutable.WrappedArray.ofRef.class, Object[].class};
        JavaSparkContext sparkContextRegisteredClasses =
                getKryoSerializationContext(registeredClasses, new Tuple2<>("spark.kryo.registrationRequired", "true"));
        sparkContextRegisteredClasses.setCheckpointDir(checkpointDir);
        JavaRDD<Person> peopleRDDRegistered = sparkContextRegisteredClasses.parallelize(personData);
        // Mark as to checkpoint and trigger an action
        peopleRDDRegistered.checkpoint();
        peopleRDDRegistered.count();
        // Get checkpointed directory
        String registeredCheckpointedDir = peopleRDDRegistered.getCheckpointFile().get();

        // Now, we can do the same but for Kryo with unregistered classes
        JavaSparkContext sparkContextUnregisteredClasses =
                getKryoSerializationContext(new Class[]{}, new Tuple2<>("spark.kryo.registrationRequired", "false"));
        sparkContextUnregisteredClasses.setCheckpointDir("/tmp/spark/checkpoints");
        JavaRDD<Person> peopleRDDUnregistered = sparkContextUnregisteredClasses.parallelize(personData);

        peopleRDDUnregistered.checkpoint();
        peopleRDDUnregistered.count();

        String unregisteredCheckpointedDir = peopleRDDUnregistered.getCheckpointFile().get();

        // Finally, check the size of serialized objects with Java serialization
        JavaSparkContext sparkContextJavaSerialization = getJavaSerializationContext();
        sparkContextJavaSerialization.setCheckpointDir(checkpointDir);
        JavaRDD<Person> peopleRDDJavaSerialization =
                sparkContextJavaSerialization.parallelize(personData);

        peopleRDDJavaSerialization.checkpoint();
        peopleRDDJavaSerialization.count();

        String javaSerializedCheckpointedDir = peopleRDDJavaSerialization.getCheckpointFile().get();

        // Now, compare sizes
        long sizeRegistered = FileUtils.sizeOfDirectory(new File(registeredCheckpointedDir.replace("file:", "")));
        long sizeUnregistered = FileUtils.sizeOfDirectory(new File(unregisteredCheckpointedDir.replace("file:", "")));
        long sizeJavaSerialized = FileUtils.sizeOfDirectory(new File(javaSerializedCheckpointedDir.replace("file:", "")));

        // Serialization with Kryo's registration should be the smallest file
        assertThat(sizeRegistered).isLessThan(sizeUnregistered);
        assertThat(sizeRegistered).isLessThan(sizeJavaSerialized);
        // Kroy without registration takes more place than Java serialization
        assertThat(sizeJavaSerialized).isLessThan(sizeUnregistered);
    }

    @Test
    public void should_fail_on_executing_function_on_not_serializable_object() {
        // Everything is serialized, at all levels. So an error will be thrown
        // even if closure or function is serialized and objects it uses is not.
        JavaSparkContext sparContext = getJavaSerializationContext();
        JavaRDD<PersonNotSerializable> personRDD =
                sparContext.parallelize(Collections.singletonList(new PersonNotSerializable()));
        try {
            personRDD.filter(new NotNullPersonPredicate());

            personRDD.count();
            fail("Should fail on working on serializable function with " +
                    "not serializable object");
        } catch (Exception e) {
            assertThat(e.getMessage())
                    .contains("object not serializable (class: com.waitingforcode.serialization.SerializationTest$PersonNotSerializable,");
        }
    }

    // Mark function as serializable to prove that it works on a not serializable object
    private static class NotNullPersonPredicate implements Function<PersonNotSerializable, Boolean>, Serializable {

        @Override
        public Boolean call(PersonNotSerializable personNotSerializable) throws Exception {
            return personNotSerializable != null;
        }
    }


    private static JavaSparkContext getJavaSerializationContext() {
        return new JavaSparkContext(CONFIGURATION);
    }

    private static JavaSparkContext getKryoSerializationContext(Class<?>[] registeredClasses, Tuple2<String, String>...configEntries) {
        SparkConf configuration = new SparkConf().setAppName("Serialization Test").setMaster("local[1]")
                .set("spark.ui.enabled", "false")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        for (Tuple2<String, String> configEntry : configEntries) {
            configuration.set(configEntry._1(), configEntry._2());
        }
        if (registeredClasses.length > 0) {
            configuration.registerKryoClasses(registeredClasses);
        }
        return new JavaSparkContext(configuration);
    }

    public static class Person implements Serializable {

        private String name;

        private int age;

        private Address address;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        public static Person valueOf(String name, int age, Address address) {
            Person person = new Person();
            person.name = name;
            person.age = age;
            person.address = address;
            return person;
        }
    }

    public static class Address implements Serializable {
        private String street;

        private String city;

        public String getStreet() {
            return street;
        }

        public void setStreet(String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public static Address valueOf(String street, String city) {
            Address address = new Address();
            address.street = street;
            address.city = city;
            return address;
        }
    }

    public static class PersonNotSerializable {}

}
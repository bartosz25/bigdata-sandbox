package com.waitingforcode.sql.udt;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class UserDefiendTypeTest {

    @Test
    public void should_create_schema_programatically_with_metadata_and_UDT() {
        // If City wouldn't be annotated with @SQLUserDefinedType, below
        // use of UDT registry should be enough to make it work
        // UDTRegistration.register("com.waitingforcode.sql.udt.City", "com.waitingforcode.sql.udt.CityUserDefinedType");
        SparkSession session = SparkSession.builder().master("local[1]").appName("UserDefinedType Test").getOrCreate();
        try (JavaSparkContext closeableContext = new JavaSparkContext(session.sparkContext())) {
            City paris = City.valueOf("Paris", 75);
            City marseille = City.valueOf("Marseille", 13);
            City nantes = City.valueOf("Nantes", 44);
            List<City> cities = Lists.newArrayList(paris, marseille, nantes);
            JavaRDD<Row> citiesRdd = closeableContext.parallelize(cities)
                    .map(RowFactory::create);

            StructType citySchema = new StructType().add("city", new CityUserDefinedType(), false);
            Dataset<Row> datasetFromRdd = session.createDataFrame(citiesRdd, citySchema);
            datasetFromRdd.show(false);

            // First, make some checks on used schema
            scala.collection.immutable.List<StructField> fields = citySchema.toList();
            assertThat(fields.size()).isEqualTo(1);
            StructField cityBeanField = fields.apply(citySchema.getFieldIndex("city").get());
            DataType dataType = cityBeanField.dataType();
            assertThat(dataType).isInstanceOf(CityUserDefinedType.class);
            // Next, check if data was correctly serialized/deserialized
            List<Row> rows = datasetFromRdd.collectAsList();
            City parisRow = getCityByName(rows, paris.getName());
            assertRowEqualsToObject(parisRow, paris);
            City marseilleRow = getCityByName(rows, marseille.getName());
            assertRowEqualsToObject(marseilleRow, marseille);
            City nantesRow = getCityByName(rows, nantes.getName());
            assertRowEqualsToObject(nantesRow, nantes);
            assertThat(rows).hasSize(3);
        }
    }

    private void assertRowEqualsToObject(City row, City city) {
        assertThat(row.getDepartmentNumber()).isEqualTo(city.getDepartmentNumber());
    }

    private City getCityByName(Collection<Row> rows, String name) {
        return rows.stream().map(row -> (City) row.getAs("city")).filter(city -> city.getName().equals(name))
                .findFirst().get();
    }

}

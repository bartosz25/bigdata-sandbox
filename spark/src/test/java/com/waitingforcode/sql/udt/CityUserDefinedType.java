package com.waitingforcode.sql.udt;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

public class CityUserDefinedType extends UserDefinedType<City> {

    private static final int DEPT_NUMBER_INDEX = 0;
    private static final int NAME_INDEX = 1;
    private static final DataType DATA_TYPE;
    static {
        MetadataBuilder metadataBuilder = new MetadataBuilder();
        metadataBuilder.putLong("maxNumber", 99);
        DATA_TYPE = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("departmentNumber", DataTypes.IntegerType, false, metadataBuilder.build()),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        });
    }

    @Override
    public DataType sqlType() {
        return DATA_TYPE;
    }

    @Override
    public InternalRow serialize(City city) {
        InternalRow row = new GenericInternalRow(2);
        row.setInt(DEPT_NUMBER_INDEX, city.getDepartmentNumber());
        row.update(NAME_INDEX, UTF8String.fromString(city.getName()));
        return row;
    }

    @Override
    public City deserialize(Object datum) {
        if (datum instanceof InternalRow) {
            InternalRow row = (InternalRow) datum;
            return City.valueOf(row.getString(NAME_INDEX), row.getInt(DEPT_NUMBER_INDEX));
        }
        throw new IllegalStateException("Unsupported conversion");
    }

    @Override
    public Class<City> userClass() {
        return City.class;
    }
}

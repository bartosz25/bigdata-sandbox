package com.waitingforcode.sql.udt;

import com.google.common.base.MoreObjects;
import org.apache.spark.sql.types.SQLUserDefinedType;

import java.io.Serializable;

@SQLUserDefinedType(udt = CityUserDefinedType.class)
public class City implements Serializable {
    private String name;

    private Integer departmentNumber;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getDepartmentNumber() {
        return departmentNumber;
    }

    public void setDepartmentNumber(Integer departmentNumber) {
        this.departmentNumber = departmentNumber;
    }

    public static City valueOf(String name, Integer departmentNumber) {
        City city = new City();
        city.name = name;
        city.departmentNumber = departmentNumber;
        return city;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("name", name).add("departmentNumber", departmentNumber)
                .toString();
    }

}

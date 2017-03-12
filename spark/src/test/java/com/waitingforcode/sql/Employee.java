package com.waitingforcode.sql;

public class Employee {

    private String name;

    private int age;

    private int annualSalary;

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

    public int getAnnualSalary() {
        return annualSalary;
    }

    public void setAnnualSalary(int annualSalary) {
        this.annualSalary = annualSalary;
    }

    public static Employee valueOf(String name, int age, int salary) {
        Employee employee = new Employee();
        employee.name = name;
        employee.age = age;
        employee.annualSalary = salary;
        return employee;
    }

}

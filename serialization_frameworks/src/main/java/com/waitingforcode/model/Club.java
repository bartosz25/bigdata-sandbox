package com.waitingforcode.model;

import com.google.common.base.MoreObjects;

public class Club {

    private String fullName;

    private int foundationYear;

    private Integer disappearanceYear;

    public Club() {
        //
    }

    public Club(String fullName, int foundationYear, Integer disappearanceYear) {
        this.fullName = fullName;
        this.foundationYear = foundationYear;
        this.disappearanceYear = disappearanceYear;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getFullName() {
        return fullName;
    }

    public int getFoundationYear() {
        return foundationYear;
    }

    public Integer getDisappearanceYear() {
        return disappearanceYear;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("fullName", fullName).add("foundationYear", foundationYear)
                .add("disappearanceYear", disappearanceYear).toString();
    }

}

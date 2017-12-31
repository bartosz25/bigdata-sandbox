package com.waitingforcode.model;

import com.google.common.base.MoreObjects;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;

public class WorkingCitizen {

    public static final Schema AVRO_SCHEMA = new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"WorkingCitizen\"," +
            "\"namespace\":\"com.waitingforcode.model\", \"fields\":[" +
            "{\"name\":\"professionalSkills\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}," +
            "{\"name\":\"professionsPerYear\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}," +
            "{\"name\":\"civility\",\"type\":{\"type\":\"enum\",\"name\":\"Civilities\"," +
            "\"symbols\":[\"MR\",\"MS\",\"MISS\",\"MRS\"]}}," +
            "{\"name\":\"firstName\",\"type\":\"string\"}," +
            "{\"name\":\"lastName\",\"type\":\"string\"}," +
            "{\"name\":\"creditRating\",\"type\":\"double\"}," +
            "{\"name\":\"isParent\",\"type\":\"boolean\"}]" +
            "}");

    private Civilities civility;

    private String firstName;

    private String lastName;

    private Double creditRating;

    private Boolean isParent;

    private List<String> professionalSkills;

    private Map<String, String> professionsPerYear;

    public Civilities getCivility() {
        return civility;
    }

    public void setCivility(Civilities civility) {
        this.civility = civility;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Double getCreditRating() {
        return creditRating;
    }

    public void setCreditRating(Double creditRating) {
        this.creditRating = creditRating;
    }

    public Boolean getParent() {
        return isParent;
    }

    public void setParent(Boolean parent) {
        isParent = parent;
    }

    public List<String> getProfessionalSkills() {
        return professionalSkills;
    }

    public void setProfessionalSkills(List<String> professionalSkills) {
        this.professionalSkills = professionalSkills;
    }

    public Map<String, String> getProfessionsPerYear() {
        return professionsPerYear;
    }

    public void setProfessionsPerYear(Map<String, String> professionsPerYear) {
        this.professionsPerYear = professionsPerYear;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("civility", civility).add("firstName", firstName)
                .add("lastName", lastName).add("creditRating", creditRating).add("isParent", isParent)
                .add("professionalSkills", professionalSkills).add("professionsPerYear", professionsPerYear).toString();
    }

}

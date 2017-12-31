package com.waitingforcode.model;

import avro.shaded.com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;

import java.util.concurrent.ThreadLocalRandom;

public class WorkingCitizenCreator {

    public static WorkingCitizen getSampleWorkingCitizen(Civilities civilities) {
        int randomMarker = ThreadLocalRandom.current().nextInt(10000);
        return getSampleWorkingCitizen(civilities, randomMarker);
    }


    public static WorkingCitizen getSampleWorkingCitizen(Civilities civilities, int randomMarker) {
        WorkingCitizen workingCitizen = new WorkingCitizen();
        workingCitizen.setCivility(civilities);
        workingCitizen.setCreditRating((double)randomMarker);
        workingCitizen.setFirstName("FirstName"+randomMarker);
        workingCitizen.setLastName("LastName"+randomMarker);
        workingCitizen.setParent(randomMarker%2 == 0);
        workingCitizen.setProfessionalSkills(Lists.newArrayList("programmer"+randomMarker, "IT director"));
        workingCitizen.setProfessionsPerYear(ImmutableMap.of("199"+randomMarker, "programmer", "199"+randomMarker+1,
                "software engineer"));
        return workingCitizen;
    }
}

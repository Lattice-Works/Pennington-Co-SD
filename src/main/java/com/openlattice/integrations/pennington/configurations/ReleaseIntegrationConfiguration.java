package com.openlattice.integrations.pennington.configurations;

public class ReleaseIntegrationConfiguration {
    private final String people;
    private final String jailStays;
    private final String subjectOf;

    public ReleaseIntegrationConfiguration(
            String people,
            String jailStays,
            String subjectOf) {
        this.people = people;
        this.jailStays = jailStays;
        this.subjectOf = subjectOf;
    }

    public String getPeople() {
        return people;
    }
    public String getJailStays() {
        return jailStays;
    }
    public String getSubjectOf() {
        return subjectOf;
    }
}

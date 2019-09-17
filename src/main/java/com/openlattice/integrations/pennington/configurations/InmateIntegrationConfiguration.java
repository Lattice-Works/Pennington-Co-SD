package com.openlattice.integrations.pennington.configurations;

public class InmateIntegrationConfiguration {
    private final String people;
    private final String bonds;
    private final String registeredfor;
    private final String bondSet;
    private final String jailStays;
    private final String subjectOf;

    public InmateIntegrationConfiguration(
            String people,
            String bonds,
            String registeredfor,
            String bondSet,
            String jailStays,
            String subjectOf ) {
        this.people = people;
        this.bonds = bonds;
        this.registeredfor = registeredfor;
        this.bondSet = bondSet;
        this.jailStays = jailStays;
        this.subjectOf = subjectOf;
    }

    public String getPeople() {
        return people;
    }

    public String getBonds() {
        return bonds;
    }

    public String getRegisteredfor() {
        return registeredfor;
    }

    public String getBondSet() {
        return bondSet;
    }

    public String getJailStays() {
        return jailStays;
    }

    public String getSubjectOf() {
        return subjectOf;
    }

}

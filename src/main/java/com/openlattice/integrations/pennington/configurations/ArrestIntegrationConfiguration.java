package com.openlattice.integrations.pennington.configurations;

public class ArrestIntegrationConfiguration {

    private final String people;
    private final String incident;
    private final String charge;
    private final String pretrialCase;
    private final String address;
    private final String contactInformation;
    private final String arrests;
    private final String chargedWith;
    private final String appearsIn;
    private final String livesAt;
    private final String contactInformationGiven;

    public ArrestIntegrationConfiguration(
            String people,
            String incident,
            String charge,
            String pretrialCase,
            String address,
            String contactInformation,
            String arrests,
            String chargedWith,
            String appearsIn,
            String livesAt, String contactInformationGiven ) {
        this.people = people;
        this.incident = incident;
        this.charge = charge;
        this.pretrialCase = pretrialCase;
        this.address = address;
        this.contactInformation = contactInformation;
        this.arrests = arrests;
        this.chargedWith = chargedWith;
        this.appearsIn = appearsIn;
        this.livesAt = livesAt;
        this.contactInformationGiven = contactInformationGiven;
    }

    public String getPeople() {
        return people;
    }

    public String getIncident() {
        return incident;
    }

    public String getCharge() {
        return charge;
    }

    public String getPretrialCase() {
        return pretrialCase;
    }

    public String getAddress() {
        return address;
    }

    public String getContactInformation() {
        return contactInformation;
    }

    public String getArrests() {
        return arrests;
    }

    public String getChargedWith() {
        return chargedWith;
    }

    public String getAppearsIn() {
        return appearsIn;
    }

    public String getLivesAt() {
        return livesAt;
    }

    public String getContactInformationGiven() {
        return contactInformationGiven;
    }
}

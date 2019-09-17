package com.openlattice.integrations.pennington.configurations;

public class ManualPSAIntegrationConfiguration {

    private final String people;
    private final String psaScores;
    private final String psaRiskFactors;
    private final String psaNotes;
    private final String rcmResults;
    private final String rcmRiskFactors;
    private final String manualPretrialCase;
    private final String manualCharges;
    private final String staff;
    private final String assessedBy;
    private final String calculatedFor;
    private final String chargedWith;
    private final String appearsIn;


    public ManualPSAIntegrationConfiguration(
            String people,
            String psaScores,
            String psaRiskFactors,
            String psaNotes,
            String rcmResults,
            String rcmRiskFactors,
            String manualPretrialCase,
            String manualCharges,
            String staff,
            String assessedBy,
            String calculatedFor,
            String chargedWith,
            String appearsIn
    ) {
        this.people = people;
        this.psaScores = psaScores;
        this.psaRiskFactors = psaRiskFactors;
        this.psaNotes = psaNotes;
        this.rcmResults = rcmResults;
        this.rcmRiskFactors = rcmRiskFactors;
        this.manualPretrialCase = manualPretrialCase;
        this.manualCharges = manualCharges;
        this.staff = staff;
        this.assessedBy = assessedBy;
        this.calculatedFor = calculatedFor;
        this.chargedWith = chargedWith;
        this.appearsIn = appearsIn;
    }

    public String getPeople() {
        return people;
    }

    public String getPsaScores() {
        return psaScores;
    }

    public String getPsaRiskFactors() { return psaRiskFactors; }

    public String getPsaNotes() { return psaNotes; }

    public String getRcmResults() { return rcmResults; }

    public String getRcmRiskFactors() { return rcmRiskFactors; }

    public String getManualPretrialCase() {
        return manualPretrialCase;
    }

    public String getManualCharges() {
        return manualCharges;
    }

    public String getStaff() { return staff; }

    public String getAssessedBy() { return assessedBy; }

    public String getChargedWith() {
        return chargedWith;
    }

    public String getCalculatedFor() { return calculatedFor; }

    public String getAppearsIn() { return appearsIn; }

}

package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.UpdateType;
import com.openlattice.integrations.pennington.utils.EdmConstants;
import com.openlattice.integrations.pennington.utils.IntegrationAliases;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Todd Bergman &lt;todd@openlattice.com&gt;
 */

enum County {
    pennington,
    minnehaha
}

public class ManualPsaIntegration {
    private static final Logger                      logger      = LoggerFactory.getLogger( ManualPsaIntegration.class );
    private static final  RetrofitFactory.Environment environment = RetrofitFactory.Environment.PROD_INTEGRATION;

    private static final JavaDateTimeHelper dtHelper_MM_dd_yyyy = new JavaDateTimeHelper( TimeZones.America_Denver,
            "MM/dd/yyyy" );

    private static final JavaDateTimeHelper dtHelper_yyyy_MM_dd_HH_mm_ss = new JavaDateTimeHelper( TimeZones.America_Denver,
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSSSSS" );

    private static final Map<County, ManualPSAIntegrationConfiguration> CONFIGURATIONS = ImmutableMap.of(
            County.pennington, new ManualPSAIntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.PENNINGTON_PSA_ENTITY_SET,
                    EdmConstants.PENNINGTON_PSA_RISK_FACTORS_ENTITY_SET,
                    EdmConstants.PENNINGTON_PSA_NOTES_ENTITY_SET,
                    EdmConstants.PENNINGTON_RCM_RESULTS_ENTITY_SET,
                    EdmConstants.PENNINGTON_RCM_RISK_FACTORS_ENTITY_SET,
                    EdmConstants.PENNINGTON_MANUAL_PRETRIAL_CASES_ENTITY_SET,
                    EdmConstants.PENNINGTON_MANUAL_CHARGES_ENTITY_SET,
                    EdmConstants.PENNINGTON_STAFF_ENTITY_SET,
                    EdmConstants.PENNINGTON_ASSESSED_BY_ENTITY_SET,
                    EdmConstants.PENNINGTON_CALCULATED_FOR_ENTITY_SET,
                    EdmConstants.CHARGED_WITH_ENTITY_SET,
                    EdmConstants.PENNINGTON_APPEARS_IN_ENTITY_SET
            ),
            County.minnehaha, new ManualPSAIntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.MINNEHAHA_PSA_ENTITY_SET,
                    EdmConstants.MINNEHAHA_PSA_RISK_FACTORS_ENTITY_SET,
                    EdmConstants.MINNEHAHA_PSA_NOTES_ENTITY_SET,
                    EdmConstants.MINNEHAHA_RCM_RESULTS_ENTITY_SET,
                    EdmConstants.MINNEHAHA_RCM_RISK_FACTORS_ENTITY_SET,
                    EdmConstants.MINNEHAHA_MANUAL_PRETRIAL_CASES_ENTITY_SET,
                    EdmConstants.MINNEHAHA_MANUAL_CHARGES_ENTITY_SET,
                    EdmConstants.MINNEHAHA_STAFF_ENTITY_SET,
                    EdmConstants.MINNEHAHA_ASSESSED_BY_ENTITY_SET,
                    EdmConstants.MINNEHAHA_CALCULATED_FOR_ENTITY_SET,
                    EdmConstants.CHARGED_WITH_ENTITY_SET,
                    EdmConstants.MINNEHAHA_APPEARS_IN_ENTITY_SET
            )
    );

    private static final Map<County, String> EMAILS = ImmutableMap.of(
            County.pennington, "mark.hirsch@pennco.org" ,
            County.minnehaha, "jingalls@minnehahacounty.org"
    );

    private static final Map<County, String> COUNTIES = ImmutableMap.of(
            County.pennington, "Court (Pennington)" ,
            County.minnehaha, "Court (Minnehaha)"
    );


    //    private static final Pattern    statuteMatcher = Pattern.compile( "([0-9+]\s\-\s(.+)\s(\((.*?)\))" ); //start with a number followed by anything, even empty string. after dash, at least 1 char, 1 whitespace, 2 parentheses
    // with anything (even nothing) in between them

    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String psaPath = args[ 2 ];

        String jwtToken = MissionControl.getIdToken( username, password );

        SimplePayload payload = new SimplePayload( psaPath );

        final ManualPSAIntegrationConfiguration config = CONFIGURATIONS.get( County.valueOf( args[ 3 ] ) );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight psaFlight = Flight.newFlight()
                .createEntities()

                .addEntity( IntegrationAliases.PERSON_ALIAS)
                    .to( config.getPeople() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.PERSON_ID_FQN, IntegrationAliases.PERSON_ID_COL )
                    .addProperty( EdmConstants.LAST_NAME_FQN ).value( row -> Parsers.getAsString( row.getAs( IntegrationAliases.LAST_NAME_COLUMN ) ) ).ok()
                    .addProperty( EdmConstants.FIRST_NAME_FQN ).value( row -> Parsers.getAsString( row.getAs( IntegrationAliases.FIRST_NAME_COLUMN ) ) ).ok()
                    .addProperty( EdmConstants.RACE_FQN )
                        .value( ManualPsaIntegration::standardRaceList).ok()
                    .addProperty( EdmConstants.GENDER_FQN)
                        .value( ManualPsaIntegration::standardSex ).ok()
                    .addProperty( EdmConstants.ETHNICITY_FQN )
                        .value( ManualPsaIntegration::standardEthnicity).ok()
                    .addProperty( EdmConstants.DOB_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDate( row.getAs( IntegrationAliases.DOB_COLUMN ) ) ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .to( config.getPsaScores() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get(IntegrationAliases.PSA_ID_COLUMN ) ) )
                    .addProperty( EdmConstants.FTA_SCORE_FQN, IntegrationAliases.FTA_COLUMN )
                    .addProperty( EdmConstants.NVCA_FLAG_FQN, IntegrationAliases.NVCA_COLUMN )
                    .addProperty( EdmConstants.NCA_SCORE_FQN, IntegrationAliases.NCA_COLUMN )
                    .addProperty( EdmConstants.STATUS_FQN ).value( row -> "Open" ).ok()
                    .addProperty( EdmConstants.GENERAL_ID_FQN, IntegrationAliases.PSA_ID_COLUMN )
                    .addProperty( EdmConstants.DATETIME_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN ) ) ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.PSA_RISK_FACTORS_ALIAS )
                    .to( config.getPsaRiskFactors() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get(IntegrationAliases.PSA_RISK_FACTORS_ID_COLUMN ) ) )
                    .addProperty( EdmConstants.PRIOR_F_CONVICTION_FQN, IntegrationAliases.PRIOR_FELONY_COLUMN )
                    .addProperty( EdmConstants.PRIOR_M_CONVICTION_FQN, IntegrationAliases.PRIOR_MISEMEANOR_COLUMN )
                    .addProperty( EdmConstants.PRIOR_SENTENCE_TO_INCARCERATION_FQN, IntegrationAliases.PRIOR_SENTENCE_TO_INCARCERATION_COLUMN )
                    .addProperty( EdmConstants.PRIOR_V_CONVICTION_FQN ).value( ManualPsaIntegration::getPriorViolentConvictionField ).ok()
                    .addProperty( EdmConstants.RECENT_FTA_FQN).value( ManualPsaIntegration::getRecentFtaField ).ok()
                    .addProperty( EdmConstants.OLD_FTA_FQN , IntegrationAliases.OLD_FTA_COLUMN )
                    .addProperty( EdmConstants.PENDING_CHARGES_FQN , IntegrationAliases.PENDING_CHARGES_COLUMN )
                    .addProperty( EdmConstants.CONTEXT_FQN ).value( row -> County.valueOf( args[ 3 ] ) ).ok()
                    .addProperty( EdmConstants.CURRENT_V_OFFENSE_FQN , IntegrationAliases.CURRENT_VIOLENT_OFFENSE_COLUMN )
                    .addProperty( EdmConstants.AGE_AT_ARREST_FQN ).value( ManualPsaIntegration::getAgeAtCurrentArrestField ).ok()
                    .addProperty( EdmConstants.VIOLENT_AND_YOUNG_FQN )
                        .value( row ->  Parsers.parseInt( row.getAs( IntegrationAliases.AGE_AT_ARREST_COLUMN ) ) < 20 && Parsers.parseBoolean( row.getAs( IntegrationAliases.CURRENT_VIOLENT_OFFENSE_COLUMN ) )).ok()
                    .addProperty( EdmConstants.GENERAL_ID_FQN,  IntegrationAliases.PSA_RISK_FACTORS_ID_COLUMN )
                .endEntity()
                .addEntity( IntegrationAliases.PSA_NOTES_ALIAS )
                    .to( config.getPsaNotes() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get(IntegrationAliases.PSA_NOTES_ID_COLUMN ) ) )
                    .addProperty( EdmConstants.RECOMMENDATION_FQN, IntegrationAliases.PSA_COMMENTS_COLUMN )
                    .addProperty( EdmConstants.GENERAL_ID_FQN, IntegrationAliases.PSA_NOTES_ID_COLUMN )
                .endEntity()
                .addEntity( IntegrationAliases.RCM_ALIAS )
                    .to( config.getRcmResults() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get(IntegrationAliases.RCM_ID_COLUMN ) ) )
                    .addProperty( EdmConstants.RELEASE_TYPE_FQN, IntegrationAliases.RELEASE_TYPE_COLUMN )
                    .addProperty( EdmConstants.COLOR_FQN, IntegrationAliases.RCM_COLOR_COLUMN )
                    .addProperty( EdmConstants.CONDITION_1_FQN, IntegrationAliases.CONDITION_ONE_COLUMN )
                    .addProperty( EdmConstants.CONDITION_2_FQN, IntegrationAliases.CONDITION_TWO_COLUMN )
                    .addProperty( EdmConstants.CONDITION_3_FQN, IntegrationAliases.CONDITION_THREE_COLUMN )
                    .addProperty( EdmConstants.GENERAL_ID_FQN, IntegrationAliases.RCM_ID_COLUMN )
                .endEntity()
                .addEntity( IntegrationAliases.RCM_RISK_FACTORS_ALIAS )
                    .to( config.getRcmRiskFactors() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get(IntegrationAliases.RCM_RISK_FACTORS_ID_COLUMN ) ) )
                    .addProperty( EdmConstants.STEP_2_FQN, IntegrationAliases.STEP2_COLUMN )
                    .addProperty( EdmConstants.STEP_4_FQN, IntegrationAliases.STEP4_COLUMN )
                    .addProperty( EdmConstants.CONTEXT_FQN ).value( row -> County.valueOf( args[ 3 ] ) ).ok()
                    .addProperty( EdmConstants.GENERAL_ID_FQN, IntegrationAliases.RCM_RISK_FACTORS_ID_COLUMN )
                .endEntity()
                .addEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .to( config.getManualPretrialCase() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get(IntegrationAliases.ARREST_CASE_NUMBER_COLUMN ) ) )
                    .addProperty( EdmConstants.CASE_NO_FQN).value( row -> Parsers.getAsString( row.getAs(IntegrationAliases.ARREST_CASE_NUMBER_COLUMN ) ) ).ok()
                    .addProperty( EdmConstants.ARREST_DATETIME_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.ARREST_DATE_COLUMN )) ).ok()
                    .addProperty( EdmConstants.FILE_DATE_FQN )
                        .value( row -> dtHelper_yyyy_MM_dd_HH_mm_ss.parseDateTime( row.getAs( IntegrationAliases.FILE_DATE )) ).ok()
                    .addProperty( EdmConstants.NUM_OF_CHARGES_FQN ).value( row -> Parsers.parseInt( row.getAs( IntegrationAliases.CHARGE_COUNT_COLUMN ) ) ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.CHARGE_ALIAS )
                    .to( config.getManualCharges() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.CHARGE_ID_FQN ).value( ManualPsaIntegration::getChargeId ).ok()
                    .addProperty( EdmConstants.CHARGE_LEVEL_FQN, IntegrationAliases.CHARGE_LEVEL_COLUMN)
                    .addProperty( EdmConstants.CHARGE_STATUTE_FQN, IntegrationAliases.STATUTE_COLUMN)
                    .addProperty( EdmConstants.CHARGE_DESCRIPTION_FQN, IntegrationAliases.DESCRIPTION_COLUMN)
                    .addProperty( EdmConstants.NUM_OF_COUNTS_FQN, IntegrationAliases.COUNT_COLUMN )
                    .addProperty( EdmConstants.COMMENTS_FQN, IntegrationAliases.OFFENSE_NOTES_COL )
                .endEntity()
                .addEntity( IntegrationAliases.STAFF_ALIAS )
                    .to( config.getStaff() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.PERSON_ID_FQN ).value( row -> EMAILS.get( County.valueOf( args[ 3 ] ) ) ).ok()
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .toEntity( IntegrationAliases.PERSON_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .toEntity( IntegrationAliases.PSA_RISK_FACTORS_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .toEntity( IntegrationAliases.RCM_RISK_FACTORS_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .toEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_RISK_FACTORS_ALIAS )
                    .toEntity( IntegrationAliases.PERSON_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_RISK_FACTORS_ALIAS )
                    .toEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_NOTES_ALIAS )
                    .toEntity( IntegrationAliases.PERSON_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_NOTES_ALIAS )
                    .toEntity( IntegrationAliases.PSA_RISK_FACTORS_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_NOTES_ALIAS )
                    .toEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_NOTES_ALIAS )
                    .toEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.RCM_RISK_FACTORS_ALIAS )
                    .toEntity( IntegrationAliases.PERSON_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.RCM_RISK_FACTORS_ALIAS )
                    .toEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.RCM_ALIAS )
                    .toEntity( IntegrationAliases.PERSON_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.RCM_ALIAS )
                    .toEntity( IntegrationAliases.RCM_RISK_FACTORS_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.RCM_ALIAS )
                    .toEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CALCULATED_FOR_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getCalculatedFor() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.RCM_ALIAS )
                    .toEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .addProperty( EdmConstants.TIMESTAMP_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.ASSESSED_BY_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getAssessedBy() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_SCORES_ALIAS )
                    .toEntity( IntegrationAliases.STAFF_ALIAS )
                    .addProperty( EdmConstants.COMPLETED_DATETIME_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.ASSESSED_BY_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getAssessedBy() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_RISK_FACTORS_ALIAS )
                    .toEntity( IntegrationAliases.STAFF_ALIAS )
                    .addProperty( EdmConstants.COMPLETED_DATETIME_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.ASSESSED_BY_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getAssessedBy() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.RCM_ALIAS )
                    .toEntity( IntegrationAliases.STAFF_ALIAS )
                    .addProperty( EdmConstants.COMPLETED_DATETIME_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.ASSESSED_BY_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getAssessedBy() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.RCM_RISK_FACTORS_ALIAS )
                    .toEntity( IntegrationAliases.STAFF_ALIAS )
                    .addProperty( EdmConstants.COMPLETED_DATETIME_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.ASSESSED_BY_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getAssessedBy() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> row.get( IntegrationAliases.PSA_DATE_COLUMN  ).toString() )
                    .fromEntity( IntegrationAliases.PSA_NOTES_ALIAS )
                    .toEntity( IntegrationAliases.STAFF_ALIAS )
                    .addProperty( EdmConstants.COMPLETED_DATETIME_FQN )
                        .value( row -> dtHelper_MM_dd_yyyy.parseDateAsDateTime( row.getAs( IntegrationAliases.PSA_DATE_COLUMN )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getAppearsIn() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get( IntegrationAliases.ARREST_CASE_NUMBER_COLUMN  ) ) )
                    .fromEntity( IntegrationAliases.CHARGE_ALIAS )
                    .toEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> Parsers.getAsString( row.getAs(IntegrationAliases.ARREST_CASE_NUMBER_COLUMN ) ) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getAppearsIn() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get( IntegrationAliases.ARREST_CASE_NUMBER_COLUMN  ) ) )
                    .fromEntity( IntegrationAliases.PERSON_ALIAS )
                    .toEntity( IntegrationAliases.MANUAL_PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> Parsers.getAsString( row.getAs(IntegrationAliases.ARREST_CASE_NUMBER_COLUMN ) ) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.CHARGED_WITH_ALIAS + UUID.randomUUID().toString() )
                    .to( config.getChargedWith() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get( IntegrationAliases.ARREST_CASE_NUMBER_COLUMN  ) ) )
                    .fromEntity( IntegrationAliases.PERSON_ALIAS )
                    .toEntity( IntegrationAliases.CHARGE_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> Parsers.getAsString( row.getAs(IntegrationAliases.ARREST_CASE_NUMBER_COLUMN ) ) ).ok()
                .endAssociation()
                .endAssociations()
                .done();
                //@formatter:on

        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com" );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( psaFlight, payload );

        missionControl.prepare( flights, false, ImmutableSet.of() ).launch( 10000 );
        MissionControl.succeed();
    }


    public static String getChargeId( Row row ) {
        String caseNumber = Parsers.getAsString( row.getAs( IntegrationAliases.ARREST_CASE_NUMBER_COLUMN ) );
        String chargeNum = Parsers.getAsString( row.getAs( IntegrationAliases.CASE_NO_COL ) );

        return caseNumber + "|" + chargeNum;
    }

    public static String getRecentFtaField( Row row ) {
        Integer recentFTACount = Parsers.parseInt( row.getAs( IntegrationAliases.RECENT_FTA_COLUMN ) );


        if ( recentFTACount != null && recentFTACount > 1 ) { return "2 or more"; };

        return Parsers.getAsString( recentFTACount );
    }

    public static String getPriorViolentConvictionField( Row row ) {
        Integer priorViolentConviction = Parsers.parseInt( row.getAs( IntegrationAliases.PRIOR_VIOLENT_COLUMN ) );


        if ( priorViolentConviction != null && priorViolentConviction > 2 ) { return "3 or more"; };

        return Parsers.getAsString( priorViolentConviction );
    }

    public static String getAgeAtCurrentArrestField( Row row ) {
        Integer ageAtCurrentArrest = Parsers.parseInt( row.getAs( IntegrationAliases.AGE_AT_ARREST_COLUMN ) );


        if ( ageAtCurrentArrest != null) {
            if (ageAtCurrentArrest < 21 ) { return "20 or Younger"; };
            if (ageAtCurrentArrest > 20 && ageAtCurrentArrest < 23) { return "21 or 22"; };
            if (ageAtCurrentArrest > 22 ) { return "23 or Older"; };
        }

        return null;
    }

    public static List standardRaceList( Row row ) {
        String sr = row.getAs( IntegrationAliases.RACE_COL );

        if ( sr != null ) {

            String[] racesArray = StringUtils.split( sr, "," );
            List<String> races = Arrays.asList( racesArray );

            if ( races != null ) {
                Collections.replaceAll( races, "Asian", "asian" );
                Collections.replaceAll( races, "White", "white" );
                Collections.replaceAll( races, "Black", "black" );
                Collections.replaceAll( races, "American Indian/Alaskan Native", "amindian" );
                Collections.replaceAll( races, "Native Hawaiian or Other Pacific Islander", "pacisland" );
                Collections.replaceAll( races, "Not Specified", "" );
                Collections.replaceAll( races, "", "" );

                List<String> finalRaces = races
                        .stream()
                        .filter( StringUtils::isNotBlank )
                        .collect( Collectors.toList() );

                return finalRaces;
            }
            return null;
        }
        return null;
    }

    public static String standardEthnicity( Row row ) {
        String eth = row.getAs( IntegrationAliases.ETHNICITY_COLUMN );

        if ( eth != null ) {
            if ( eth.equals( "Hispanic" ) ) { return "hispanic"; }
            if ( eth.equals( "Not Hispanic" ) ) { return "nonhispanic"; }
            if ( eth.equals( "Not Specified" ) ) { return ""; }
            if ( eth.equals( "Unknown" ) ) { return ""; }
            if ( eth.equals( "" ) ) { return null; }
        }

        return null;
    }

    public static String standardSex( Row row ) {
        String sex = row.getAs( IntegrationAliases.GENDER_COLUMN );

        if ( sex != null ) {
            if ( sex.equals( "Male" ) ) {return "M"; }
            if ( sex.equals( "Female" ) ) {return "F"; }
            if ( sex.equals( "" ) ) { return null; }

        }
        return null;

    }

}

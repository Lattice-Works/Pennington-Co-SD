package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.UpdateType;
import com.openlattice.integrations.pennington.utils.EdmConstants;
import com.openlattice.integrations.pennington.utils.IntegrationAliases;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
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
import java.util.stream.Stream;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

enum County {
    pennington,
    minnehaha,
    tto
}

public class ZuercherArrest {
    private static final Logger                      logger      = LoggerFactory.getLogger( ZuercherArrest.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    private static final DateTimeHelper     bdHelper = new DateTimeHelper( TimeZones.America_Denver,
            "MM/dd/YY" );
    private static final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( TimeZones.America_Denver,
            "MM/dd/yy HH:mm" );

    private static final Map<County, IntegrationConfiguration> CONFIGURATIONS = ImmutableMap.of(
            County.pennington, new IntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    "PenZuercherIncident",
                    "PenZuercherCharge",
                    "PenZuercherPretrialCase",
                    "PenZuercherAddress",
                    EdmConstants.CONTACT_INFO_ENTITY_SET,
                    "PenZuercherArrests",
                    "PenZuercherchargedwith",
                    "PenZuercherAppearsin",
                    "PenZLivesAt",
                    EdmConstants.CONTACT_INFO_GIVEN_ENTITY_SET
            ),
            County.minnehaha, new IntegrationConfiguration(
                    "southdakotapeople",
                    "Minnehaha County, SD_app_incident",
                    "Minnehaha County, SD_app_arrestcharges",
                    "Minnehaha County, SD_app_arrestpretrialcases",
                    "Minnehaha County, SD_app_address",
                    EdmConstants.CONTACT_INFO_ENTITY_SET,
                    "Minnehaha County, SD_app_arrestedin",
                    "Minnehaha County, SD_app_arrestchargedwith",
                    "Minnehaha County, SD_app_appearsinarrest",
                    "Minnehaha County, SD_app_livesat_arrest",
                    EdmConstants.CONTACT_INFO_GIVEN_ENTITY_SET
            ),
            County.tto, new IntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    "tto_app_incident",
                    "tto_app_arrestcharges",
                    "tto_app_arrestpretrialcases",
                    "tto_app_address",
                    EdmConstants.CONTACT_INFO_ENTITY_SET,
                    "tto_app_arrestedin",
                    "tto_app_arrestchargedwith",
                    "tto_app_appearsinarrest",
                    "tto_app_livesat_arrest",
                    EdmConstants.CONTACT_INFO_GIVEN_ENTITY_SET
            )
    );


    //    private static final Pattern    statuteMatcher = Pattern.compile( "([0-9+]\s\-\s(.+)\s(\((.*?)\))" ); //start with a number followed by anything, even empty string. after dash, at least 1 char, 1 whitespace, 2 parentheses
    // with anything (even nothing) in between them

    public static void integrate() throws InterruptedException, IOException {

//        final String username = args[ 0 ];
//        final String password = args[ 1 ];
//        final String arrestsPath = args[ 2 ];

        final String arrestsPath = "/Users/toddbergman/Desktop/arrests.csv";
//        String jwtToken = MissionControl.getIdToken( username, password );
        final String jwtToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6InRvZGRAb3BlbmxhdHRpY2UuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInVzZXJfbWV0YWRhdGEiOnt9LCJhcHBfbWV0YWRhdGEiOnsicm9sZXMiOlsiQXV0aGVudGljYXRlZFVzZXIiLCJhZG1pbiJdfSwibmlja25hbWUiOiJ0b2RkIiwicm9sZXMiOlsiQXV0aGVudGljYXRlZFVzZXIiLCJhZG1pbiJdLCJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMTA0MDg4MTk5MDIxNTM0MzY1NzUiLCJpc3MiOiJodHRwczovL29wZW5sYXR0aWNlLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDExMDQwODgxOTkwMjE1MzQzNjU3NSIsImF1ZCI6IktUemd5eHM2S0JjSkhCODcyZVNNZTJjcFRIemh4Uzk5IiwiaWF0IjoxNTY4MTY2Mjk0LCJleHAiOjE1NjgyNTI2OTR9.8wSMIve8bMVgiYvxLr0mjAAvyHXeolZ2hqrJnilPgGM";


        Payload payload = new SimplePayload( arrestsPath );

        //        SimplePayload payload = new SimplePayload( arrestsPath );

//        final IntegrationConfiguration config = CONFIGURATIONS.get( County.valueOf( args[ 3 ] ) );
        final IntegrationConfiguration config = CONFIGURATIONS.get( County.tto );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight arrestsflight = Flight.newFlight()
                .createEntities()

                .addEntity( IntegrationAliases.ARRESTEE_ALIAS)
                    .to( config.getPeople() )
                    .updateType( UpdateType.Merge )
                    .addProperty( EdmConstants.SSN_FQN, IntegrationAliases.SSN_COL )
                    .addProperty( EdmConstants.PERSON_ID_FQN, IntegrationAliases.PERSON_ID_COL )
                    .addProperty( EdmConstants.JACKET_NO_FQN, IntegrationAliases.JACKET_NO_COL )
                    .addProperty( EdmConstants.LAST_NAME_FQN, IntegrationAliases.LAST_NAME_COL )
                    .addProperty( EdmConstants.FIRST_NAME_FQN, IntegrationAliases.FIRST_NAME_COL )
                    .addProperty( EdmConstants.MIDDLE_NAME_FQN, IntegrationAliases.MIDDLE_NAME_COL )
                    .addProperty( EdmConstants.NICKNAME_FQN ).value( ZuercherArrest::getAliases ).ok()
                    .addProperty( EdmConstants.SSN_FQN, IntegrationAliases.SSN_COL )
                    .addProperty( EdmConstants.RACE_FQN )
                        .value( ZuercherArrest::standardRaceList).ok()
                    .addProperty( EdmConstants.GENDER_FQN)
                        .value( ZuercherArrest::standardSex ).ok()
                    .addProperty( EdmConstants.ETHNICITY_FQN )
                        .value( ZuercherArrest::standardEthnicity).ok()
                    .addProperty( EdmConstants.DOB_FQN )
                        .value( row -> bdHelper.parseDate( row.getAs( IntegrationAliases.DOB_COL ) ) ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.INCIDENT_ALIAS )
                    .to( config.getIncident() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.INCIDENT_ID_FQN, IntegrationAliases.CASE_NUMBER_COL )
                    .addProperty( EdmConstants.GANG_ACTIVITY_FQN, IntegrationAliases.GANG_ACTIVITY_COL )
                    .addProperty( EdmConstants.JUVENILE_GANG_FQN, IntegrationAliases.JUVENILE_GANG_COL )
                    .addProperty( EdmConstants.START_DATETIME_FQN )
                        .value( row ->  dtHelper.parseDateTime( row.getAs( IntegrationAliases.INCIDENT_START_DATETIME_COL ) )).ok()
                    .addProperty( EdmConstants.REPORTED_DATETIME_FQN )
                        .value( row -> dtHelper.parseDateTime( row.getAs( IntegrationAliases.REPORTED_DATETIME_COL ) ) ).ok()
                    .addProperty( EdmConstants.DRUGS_PRESENT_FQN, IntegrationAliases.DRUGS_USED_COL )
                    .addProperty( EdmConstants.ALCOHOL_CRIME_FQN, IntegrationAliases.ALCOHOL_USED_COL )
                    .addProperty( EdmConstants.COMPUTER_CRIME_FQN, IntegrationAliases.COMPUTER_USED_COL )
                .endEntity()
                .addEntity( IntegrationAliases.CHARGE_ALIAS )
                    .to( config.getCharge() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.CHARGE_ID_FQN ).value( ZuercherArrest::getChargeId ).ok()
                    .addProperty( EdmConstants.CHARGE_STATUTE_FQN)
                        .value( ZuercherArrest::localStatute ).ok()
                    .addProperty( EdmConstants.CHARGE_DESCRIPTION_FQN)
                        .value( ZuercherArrest::offense ).ok()
                    .addProperty( EdmConstants.NUM_OF_COUNTS_FQN, IntegrationAliases.OFFENSE_COUNT_COL )
                    .addProperty( EdmConstants.COMMENTS_FQN, IntegrationAliases.OFFENSE_NOTES_COL )
                .endEntity()
                .addEntity( IntegrationAliases.PRETRIAL_CASE_ALIAS )
                    .to( config.getPretrialCase() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get(IntegrationAliases.ARREST_TRANSACTION_NO_COL ) ) )
                    .addProperty( EdmConstants.CASE_NO_FQN).value( row -> Parsers.getAsString( row.getAs(IntegrationAliases.ARREST_TRANSACTION_NO_COL ) ) ).ok()
                    .addProperty( EdmConstants.NAME_FQN, IntegrationAliases.CASE_NUMBER_COL )
                    .addProperty( EdmConstants.ARREST_DATETIME_FQN )
                        .value( row -> dtHelper.parseDateTime( row.getAs( IntegrationAliases.ARREST_DATETIME_COL )) ).ok()
                    .addProperty( EdmConstants.NUM_OF_CHARGES_FQN ).value( row -> Parsers.parseInt( row.getAs( IntegrationAliases.CHARGE_COUNT_COL ) ) ).ok()
                    .addProperty( EdmConstants.ARRESTING_AGENCY_FQN, IntegrationAliases.ABBREVIATION_COL )
                .endEntity()
                .addEntity( IntegrationAliases.CONTACT_INFO_ALIAS )
                    .to( config.getContactInformation() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.GENERAL_ID_FQN).value( ZuercherArrest::formatPhoneNumber ).ok()
                    .addProperty( EdmConstants.PHONE_FQN ).value( ZuercherArrest::formatPhoneNumber ).ok()
                    .addProperty( EdmConstants.CELL_PHONE_FQN, IntegrationAliases.IS_MOBILE_COL )
                    .addProperty( EdmConstants.PREFERRED_FQN, IntegrationAliases.IS_PREFERRED_COL )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( IntegrationAliases.ARRESTED_IN_ALIAS )
                    .to( config.getArrests() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                    .toEntity( IntegrationAliases.INCIDENT_ALIAS )
                    .addProperty( EdmConstants.ARREST_DATETIME_FQN )
                        .value( row -> dtHelper.parseDateTime( row.getAs( IntegrationAliases.ARREST_DATETIME_COL )) ).ok()
                    .addProperty( EdmConstants.PERSON_ID_FQN, IntegrationAliases.PERSON_ID_COL )
                    .addProperty( EdmConstants.AGE_AT_EVENT_FQN, IntegrationAliases.AGE_AT_ARREST_COL )
                    .addProperty( EdmConstants.ARRESTING_AGENCY_FQN, IntegrationAliases.ABBREVIATION_COL )
                .endAssociation()
                .addAssociation( IntegrationAliases.CHARGED_WITH_ALIAS )
                    .to( config.getChargedWith() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                    .toEntity( IntegrationAliases.CHARGE_ALIAS )
                .entityIdGenerator( row -> Optional.ofNullable( Parsers.getAsString( row.get( IntegrationAliases.SSN_COL ) ) ).orElse( "" ) + Optional.ofNullable( Parsers.getAsString( row.get( IntegrationAliases.STATUTE_COL ) ) ).orElse( "" ) )
                .addProperty( EdmConstants.STRING_ID_FQN , IntegrationAliases.PERSON_ID_COL)
                    .addProperty( EdmConstants.CHARGE_LEVEL_FQN )
                        .value( ZuercherArrest::chargeLevel ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                    .to( config.getAppearsIn() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                    .toEntity( IntegrationAliases.PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN )
                        .value( row -> Parsers.getAsString( row.getAs( IntegrationAliases.ARREST_TRANSACTION_NO_COL )) + Parsers.getAsString( row.getAs( IntegrationAliases.PERSON_ID_COL )) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.HAS_CONTACT_ALIAS )
                    .to( config.getContactInformationGiven() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                    .toEntity( IntegrationAliases.CONTACT_INFO_ALIAS )
                    .addProperty( EdmConstants.OL_ID_FQN ).value( ZuercherArrest::formatPhoneNumber ).ok()
                .endAssociation()
                .endAssociations()
                .done();
                //@formatter:on

        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com" );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( arrestsflight, payload );
        missionControl.prepare( flights, false, ImmutableList.of(), ImmutableSet.of() ).launch( 150 );
        MissionControl.succeed();
    }

    public static String formatPhoneNumber( Row row ) {
        String phoneNumber = Parsers.getAsString( row.getAs( IntegrationAliases.PHONE_COL ) );
        if ( StringUtils.isNotBlank( phoneNumber ) ) {
            String numbersOnly = phoneNumber.replaceAll( "[^0-9]", "" );
            if ( numbersOnly.length() == 10 ) {
                return new StringBuilder( "(" )
                        .append( numbersOnly, 0, 3 )
                        .append( ") " )
                        .append( numbersOnly, 3, 6 )
                        .append( "-" )
                        .append( numbersOnly, 6, 10 )
                        .toString();
            }

        }

        return null;
    }

    public static List<String> getAliases( Row row ) {
        String aliases = Parsers.getAsString( row.getAs( IntegrationAliases.ALIASES_COL ) );
        if ( StringUtils.isNotBlank( aliases ) ) {
            return Lists.newArrayList( aliases.split( "\\|" ) );
        }

        return null;
    }

    public static String getChargeId( Row row ) {
        String caseNumber = Parsers.getAsString( row.getAs( IntegrationAliases.ARREST_TRANSACTION_NO_COL ) );
        String personId = Parsers.getAsString( row.getAs( IntegrationAliases.PERSON_ID_COL ) );
        String statuteOffense = Parsers.getAsString( row.getAs( IntegrationAliases.STATUTE_COL ) );
        String chargeNum = Parsers.getAsString( row.getAs( IntegrationAliases.CHARGE_NO_COL ) );

        return caseNumber + "|" + personId + "|" + statuteOffense + "|" + chargeNum;
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
        String eth = row.getAs( IntegrationAliases.ETHNICITY_COL );

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
        String sex = row.getAs( IntegrationAliases.SEX_COL );

        if ( sex != null ) {
            if ( sex.equals( "Male" ) ) {return "M"; }
            if ( sex.equals( "Female" ) ) {return "F"; }
            if ( sex.equals( "" ) ) { return null; }

        }
        return null;

    }

    public static String localStatute( Row row ) {
        String statoff = Parsers.getAsString( row.getAs( IntegrationAliases.STATUTE_COL ) );
        if ( statoff != null ) {
            String[] statutesplit = statoff.split( " " );
            String statute = statutesplit[ 0 ];                //SAVE 1ST ELEMENT IN ARRAY AS A STRING
            if ( Character
                    .isDigit( statute.charAt( 0 ) ) ) {     //if it begins with a number, assume it's the statute #
                return statute;
            }
            return null;
        }
        return null;
    }

    public static String chargeLevel( Row row ) {
        String all = Parsers.getAsString( row.getAs( IntegrationAliases.STATUTE_COL ) );
        if ( StringUtils.isNotBlank( all ) && StringUtils.isNotBlank( all.trim() ) ) {
            all = all.trim();
            String[] splitlevel = all.split( " " );
            String charge = splitlevel[ splitlevel.length - 1 ];
            charge = charge.replace( "(", "" );
            charge = charge.replace( ")", "" );

            if ( ( charge.startsWith( "M" ) || charge.startsWith( "F" ) ) && charge.length() > 1 ) {
                return charge.substring( charge.length()
                        - 2 );    //return the last 2 characters. For cases where the last element in the array ins now, "FamilyM1", was "Family(M1)"
            }
            return null;
        }
        return null;
    }

    //split on string, get an array.
    // string 0 is statute, string 1 goes away. String length-1 is degree level, string 2-length-2 is the middle. Join on spaces (put back as string)
    public static String offense( Row row ) {
        String offenseStr = Parsers.getAsString( row.getAs( IntegrationAliases.STATUTE_COL ) );
        if ( StringUtils.isNotBlank( offenseStr ) && StringUtils.isNotBlank( offenseStr.trim() ) ) {
            offenseStr = offenseStr.trim();

            String divider = " - ";
            int index = offenseStr.indexOf( divider );
            if ( index >= 0 ) {
                offenseStr = offenseStr.substring( index + divider.length() ).trim();
            }
            return offenseStr;
        }
        return null;
    }

    //if case Number and person is the same, increment offenses. For eacn Case #/person combo, get all offenses. Make array. Create new array, fill with integers.
    //    public static Integer offenseCount( Row row) {
    //        String casenumber = row.getAs( "Case Number" );
    //        if (chargeCounts.containsKey( casenumber )) {
    //
    //        }
    //        String person = row.getAs( IntegrationAliases.SSN_COL );
    //        String offense = row.getAs( IntegrationAliases.STATUTE_COL );
    //
    //    }

}

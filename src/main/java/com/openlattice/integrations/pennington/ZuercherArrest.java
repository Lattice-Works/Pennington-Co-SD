package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.UpdateType;
import com.openlattice.integrations.pennington.configurations.ArrestIntegrationConfiguration;
import com.openlattice.integrations.pennington.utils.*;
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
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class ZuercherArrest {
    private static final Logger                      logger      = LoggerFactory.getLogger( ZuercherArrest.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PROD_INTEGRATION;

    private static final DateTimeHelper     bdHelper = new DateTimeHelper( TimeZones.America_Denver,
            "MM/dd/YY" );
    private static final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( TimeZones.America_Denver,
            "MM/dd/yy HH:mm" );


    //    private static final Pattern    statuteMatcher = Pattern.compile( "([0-9+]\s\-\s(.+)\s(\((.*?)\))" ); //start with a number followed by anything, even empty string. after dash, at least 1 char, 1 whitespace, 2 parentheses
    // with anything (even nothing) in between them

    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String arrestsPath = args[ 2 ];
        String jwtToken = MissionControl.getIdToken( username, password );

        SimplePayload payload = new SimplePayload( arrestsPath );
        final ArrestIntegrationConfiguration config = ArrestIntegrationConfigurations.CONFIGURATIONS.get( County.valueOf( args[ 3 ] ) );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight arrestsflight = Flight.newFlight()
                .createEntities()

                .addEntity( IntegrationAliases.ARRESTEE_ALIAS)
                    .to( config.getPeople() )
                    .updateType( UpdateType.Merge )
                    .addProperty( EdmConstants.PERSON_ID_FQN, ZuercherConstants.PARTY_ID )
                    .addProperty( EdmConstants.JACKET_NO_FQN, ZuercherConstants.PERSON_JACKET_ID )
                    .addProperty( EdmConstants.LAST_NAME_FQN, ZuercherConstants.LAST_NAME )
                    .addProperty( EdmConstants.FIRST_NAME_FQN, ZuercherConstants.FIRST_NAME )
                    .addProperty( EdmConstants.MIDDLE_NAME_FQN, ZuercherConstants.MIDDLE_NAME )
                    .addProperty( EdmConstants.NICKNAME_FQN ).value( ZuercherArrest::getAliases ).ok()
                    .addProperty( EdmConstants.SSN_FQN, ZuercherConstants.SSN )
                    .addProperty( EdmConstants.RACE_FQN )
                        .value( ZuercherArrest::standardRaceList).ok()
                    .addProperty( EdmConstants.GENDER_FQN)
                        .value( ZuercherArrest::standardSex ).ok()
                    .addProperty( EdmConstants.ETHNICITY_FQN )
                        .value( ZuercherArrest::standardEthnicity).ok()
                    .addProperty( EdmConstants.DOB_FQN )
                        .value( row -> bdHelper.parseDate( row.getAs( ZuercherConstants.DOB ) ) ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.INCIDENT_ALIAS )
                    .to( config.getIncident() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.INCIDENT_ID_FQN, ZuercherConstants.CASE_NUMBER )
                    .addProperty( EdmConstants.GANG_ACTIVITY_FQN, ZuercherConstants.OTHER_GANG )
                    .addProperty( EdmConstants.JUVENILE_GANG_FQN, ZuercherConstants.JUVENILE_GANG )
                    .addProperty( EdmConstants.START_DATETIME_FQN )
                        .value( row ->  dtHelper.parseDateTime( row.getAs( ZuercherConstants.INCIDENT_START_DATE ) )).ok()
                    .addProperty( EdmConstants.REPORTED_DATETIME_FQN )
                        .value( row -> dtHelper.parseDateTime( row.getAs( ZuercherConstants.REPORTED_DATE_TIME ) ) ).ok()
                    .addProperty( EdmConstants.DRUGS_PRESENT_FQN, ZuercherConstants.OFFENDER_USED_DRUGS )
                    .addProperty( EdmConstants.ALCOHOL_CRIME_FQN, ZuercherConstants.OFFENDER_USED_ALCOHOL )
                    .addProperty( EdmConstants.COMPUTER_CRIME_FQN, ZuercherConstants.OFFENDER_USED_COMPUTER )
                .endEntity()
                .addEntity( IntegrationAliases.CHARGE_ALIAS )
                    .to( config.getCharge() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.CHARGE_ID_FQN ).value( ZuercherArrest::getChargeId ).ok()
                    .addProperty( EdmConstants.CHARGE_STATUTE_FQN)
                        .value( ZuercherArrest::localStatute ).ok()
                    .addProperty( EdmConstants.CHARGE_DESCRIPTION_FQN)
                        .value( ZuercherArrest::offense ).ok()
                    .addProperty( EdmConstants.NUM_OF_COUNTS_FQN, ZuercherConstants.OFFENSE_COUNT )
                    .addProperty( EdmConstants.COMMENTS_FQN, ZuercherConstants.OFFENSE_DETAILS )
                .endEntity()
                .addEntity( IntegrationAliases.PRETRIAL_CASE_ALIAS )
                    .to( config.getPretrialCase() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get( ZuercherConstants.ARREST_TRANSACTION_NUMBER ) ) )
                    .addProperty( EdmConstants.CASE_NO_FQN).value( row -> Parsers.getAsString( row.getAs( ZuercherConstants.ARREST_TRANSACTION_NUMBER) ) ).ok()
                    .addProperty( EdmConstants.NAME_FQN, ZuercherConstants.CASE_NUMBER )
                    .addProperty( EdmConstants.ARREST_DATETIME_FQN )
                        .value( row -> dtHelper.parseDateTime( row.getAs( ZuercherConstants.ARREST_DATE_TIME )) ).ok()
                    .addProperty( EdmConstants.NUM_OF_CHARGES_FQN ).value( row -> Parsers.parseInt( row.getAs( ZuercherConstants.CHARGE_COUNT ) ) ).ok()
                    .addProperty( EdmConstants.ARRESTING_AGENCY_FQN, ZuercherConstants.ABBREVIATION )
                .endEntity()
                .addEntity( IntegrationAliases.CONTACT_INFO_ALIAS )
                    .to( config.getContactInformation() )
                    .updateType( UpdateType.Replace )
                    .addProperty( EdmConstants.GENERAL_ID_FQN).value( ZuercherArrest::formatPhoneNumber ).ok()
                    .addProperty( EdmConstants.PHONE_FQN ).value( ZuercherArrest::formatPhoneNumber ).ok()
                    .addProperty( EdmConstants.CELL_PHONE_FQN, ZuercherConstants.IS_MOBILE )
                    .addProperty( EdmConstants.TYPE_FQN, ZuercherConstants.CONTACT_TYPE )
                    .addProperty( EdmConstants.PREFERRED_FQN, ZuercherConstants.IS_PREFERRED )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( IntegrationAliases.ARRESTED_IN_ALIAS )
                    .to( config.getArrests() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                    .toEntity( IntegrationAliases.INCIDENT_ALIAS )
                    .addProperty( EdmConstants.ARREST_DATETIME_FQN )
                        .value( row -> dtHelper.parseDateTime( row.getAs( ZuercherConstants.ARREST_DATE_TIME )) ).ok()
                    .addProperty( EdmConstants.PERSON_ID_FQN, ZuercherConstants.PARTY_ID )
                    .addProperty( EdmConstants.AGE_AT_EVENT_FQN, ZuercherConstants.AGE_AT_ARREST )
                    .addProperty( EdmConstants.ARRESTING_AGENCY_FQN, ZuercherConstants.ABBREVIATION)
                .endAssociation()
                .addAssociation( IntegrationAliases.CHARGED_WITH_ALIAS )
                    .to( config.getChargedWith() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                    .toEntity( IntegrationAliases.CHARGE_ALIAS )
                .entityIdGenerator( row -> Optional.ofNullable( Parsers.getAsString( row.get( ZuercherConstants.SSN ) ) ).orElse( "" ) + Optional.ofNullable( Parsers.getAsString( row.get( ZuercherConstants.STATUTE ) ) ).orElse( "" ) )
                .addProperty( EdmConstants.STRING_ID_FQN , ZuercherConstants.PARTY_ID)
                    .addProperty( EdmConstants.CHARGE_LEVEL_FQN )
                        .value( ZuercherArrest::chargeLevel ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                    .to( config.getAppearsIn() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                    .toEntity( IntegrationAliases.PRETRIAL_CASE_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN )
                        .value( row -> Parsers.getAsString( row.getAs( ZuercherConstants.ARREST_TRANSACTION_NUMBER )) + Parsers.getAsString( row.getAs( ZuercherConstants.PARTY_ID )) ).ok()
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
        missionControl.prepare( flights, false, ImmutableList.of(), ImmutableSet.of() ).launch( 10000 );
        MissionControl.succeed();
    }

    public static String formatPhoneNumber( Row row ) {
        String phoneNumber = Parsers.getAsString( row.getAs( ZuercherConstants.PHONE ) );
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
        String aliases = Parsers.getAsString( row.getAs( ZuercherConstants.ALIASES ) );
        if ( StringUtils.isNotBlank( aliases ) ) {
            return Lists.newArrayList( aliases.split( "\\|" ) );
        }

        return null;
    }

    public static String getChargeId( Row row ) {
        String caseNumber = Parsers.getAsString( row.getAs( ZuercherConstants.ARREST_TRANSACTION_NUMBER ) );
        String personId = Parsers.getAsString( row.getAs( ZuercherConstants.PARTY_ID ) );
        String statuteOffense = Parsers.getAsString( row.getAs( ZuercherConstants.STATUTE ) );
        String chargeNum = Parsers.getAsString( row.getAs( ZuercherConstants.CHARGE_NUMBER ) );

        return caseNumber + "|" + personId + "|" + statuteOffense + "|" + chargeNum;
    }

    public static List standardRaceList( Row row ) {
        String sr = row.getAs( ZuercherConstants.RACE );

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
        String eth = row.getAs( ZuercherConstants.ETHNICITY );

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
        String sex = row.getAs( ZuercherConstants.SEX );

        if ( sex != null ) {
            if ( sex.equals( "Male" ) ) {return "M"; }
            if ( sex.equals( "Female" ) ) {return "F"; }
            if ( sex.equals( "" ) ) { return null; }

        }
        return null;

    }

    public static String localStatute( Row row ) {
        String statoff = Parsers.getAsString( row.getAs( ZuercherConstants.STATUTE ) );
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
        String all = Parsers.getAsString( row.getAs( ZuercherConstants.STATUTE ) );
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
        String offenseStr = Parsers.getAsString( row.getAs( ZuercherConstants.STATUTE ) );
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

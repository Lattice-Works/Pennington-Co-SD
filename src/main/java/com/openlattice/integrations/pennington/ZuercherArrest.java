package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.UpdateType;
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
    minnehaha
}

public class ZuercherArrest {
    private static final Logger                      logger      = LoggerFactory.getLogger( ZuercherArrest.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PROD_INTEGRATION;

    private static final DateTimeHelper     bdHelper = new DateTimeHelper( TimeZones.America_Denver,
            "MM/dd/YY" );
    private static final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( TimeZones.America_Denver,
            "MM/dd/yy HH:mm" );

    private static final Map<County, IntegrationConfiguration> CONFIGURATIONS = ImmutableMap.of(
            County.pennington, new IntegrationConfiguration(
                    "southdakotapeople",
                    "PenZuercherIncident",
                    "PenZuercherCharge",
                    "PenZuercherPretrialCase",
                    "PenZuercherAddress",
                    "southdakotacontactinformation",
                    "PenZuercherArrests",
                    "PenZuercherchargedwith",
                    "PenZuercherAppearsin",
                    "PenZLivesAt",
                    "southdakotacontactinfogiven"
            ),
            County.minnehaha, new IntegrationConfiguration(
                    "southdakotapeople",
                    "Minnehaha County, SD_app_incident",
                    "Minnehaha County, SD_app_arrestcharges",
                    "Minnehaha County, SD_app_arrestpretrialcases",
                    "Minnehaha County, SD_app_address",
                    "southdakotacontactinformation",
                    "Minnehaha County, SD_app_arrestedin",
                    "Minnehaha County, SD_app_arrestchargedwith",
                    "Minnehaha County, SD_app_appearsinarrest",
                    "Minnehaha County, SD_app_livesat_arrest",
                    "southdakotacontactinfogiven"
            )
    );

    private static final String CONTACT_INFO_NAME       = "southdakotacontactinformation";
    private static final String CONTACT_INFO_GIVEN_NAME = "southdakotacontactinfogiven";

    //    private static final Pattern    statuteMatcher = Pattern.compile( "([0-9+]\s\-\s(.+)\s(\((.*?)\))" ); //start with a number followed by anything, even empty string. after dash, at least 1 char, 1 whitespace, 2 parentheses
    // with anything (even nothing) in between them

    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String arrestsPath = args[ 2 ];
        String jwtToken = MissionControl.getIdToken( username, password );

        Payload payload = new SimplePayload( arrestsPath );

        //        SimplePayload payload = new SimplePayload( arrestsPath );
        final IntegrationConfiguration config = CONFIGURATIONS.get( County.valueOf( args[ 3 ] ) );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight arrestsflight = Flight.newFlight()
                .createEntities()

                .addEntity( "arrestee")
                    .to( config.getPeople() )
                    .updateType( UpdateType.Merge )
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.SubjectIdentification", "PartyID" )
                    .addProperty( "justice.xref", "Jacket number" )
                    .addProperty( "nc.PersonSurName", "Last Name" )
                    .addProperty( "nc.PersonGivenName", "First Name" )
                    .addProperty( "nc.PersonMiddleName", "Middle Name" )
                    .addProperty( "im.PersonNickName" ).value( ZuercherArrest::getAliases ).ok()
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.PersonRace" )
                        .value( ZuercherArrest::standardRaceList).ok()
                    .addProperty( "nc.PersonSex")
                        .value( ZuercherArrest::standardSex ).ok()
                    .addProperty( "nc.PersonEthnicity" )
                        .value( ZuercherArrest::standardEthnicity).ok()
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> bdHelper.parseDate( row.getAs("DOB" ) ) ).ok()
                .endEntity()
                .addEntity( "incident" )
                    .to( config.getIncident() )
                    .updateType( UpdateType.Replace )
                    .addProperty( "criminaljustice.incidentid", "Case Number" )
                    .addProperty( "ol.gangactivity", "Other Gang" )
                    .addProperty( "ol.juvenilegang", "Juvenile Gang" )
                    .addProperty( "ol.datetimestart" )
                        .value( row ->  dtHelper.parseDateTime( row.getAs( "Incident Start Date/Time" ) )).ok()
                    .addProperty( "ol.datetime_reported" )
                        .value( row -> dtHelper.parseDateTime( row.getAs( "Reported Date/Time" ) ) ).ok()
                    .addProperty( "publicsafety.drugspresent", "Offender(s) Used Drugs in Crime" )
                    .addProperty( "ol.alcoholincrime", "Offender(s) Used Alcohol in Crime" )
                    .addProperty( "ol.computerincrime", "Offender(s) Used Computer Equipment" )
                .endEntity()
                .addEntity( "charge" )
                    .to( config.getCharge() )
                    .updateType( UpdateType.Replace )
                    .addProperty( "justice.ArrestTrackingNumber" ).value( ZuercherArrest::getChargeId ).ok()
                    .addProperty( "event.OffenseLocalCodeSection")
                        .value( ZuercherArrest::localStatute ).ok()
                    .addProperty( "event.OffenseLocalDescription")
                        .value( ZuercherArrest::offense ).ok()
                    .addProperty( "ol.numberofcounts", "Offense Count" )
                    .addProperty( "event.comments", "Offense Details" )
                .endEntity()
                .addEntity( "pretrialcase" )
                    .to( config.getPretrialCase() )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get("Arrest Transaction number" ) ) )
                    .addProperty( "j.CaseNumberText").value( row -> Parsers.getAsString( row.getAs("Arrest Transaction number" ) ) ).ok()
                    .addProperty( "ol.name", "Case Number" )
                    .addProperty( "ol.arrestdatetime" )
                        .value( row -> dtHelper.parseDateTime( row.getAs( "Arrest Date/Time" )) ).ok()
                    .addProperty( "publicsafety.NumberOfCharges" ).value( row -> Parsers.parseInt( row.getAs( "ChargeCount" ) ) ).ok()
                    .addProperty( "criminaljustice.arrestagency", "Abbreviation" )
                .endEntity()
                .addEntity( "address" )
                    .to( config.getAddress() )
                    .updateType( UpdateType.Merge )
                    .addProperty( "location.Address")
                        .value( ZuercherArrest::getFulladdress ).ok()
                    .addProperty( "location.city", "City" )
                    .addProperty( "location.state", "State" )
                    .addProperty( "location.zip", "ZIP")
                .endEntity()
                .addEntity( "contactInfo" )
                    .to( config.getContactInformation() )
                    .updateType( UpdateType.Replace )
                    .addProperty( "general.id").value( ZuercherArrest::formatPhoneNumber ).ok()
                    .addProperty( "contact.phonenumber" ).value( ZuercherArrest::formatPhoneNumber ).ok()
                    .addProperty( "contact.cellphone", "isMobile" )
                    .addProperty( "ol.preferred", "preferred" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "arrestedin" )
                    .to( config.getArrests() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( "arrestee" )
                    .toEntity( "incident" )
                    .addProperty( "ol.arrestdatetime" )
                        .value( row -> dtHelper.parseDateTime( row.getAs( "Arrest Date/Time" )) ).ok()
                    .addProperty( "nc.SubjectIdentification", "PartyID" )
                    .addProperty( "person.ageatevent", "Age When Arrested" )
                    .addProperty( "criminaljustice.arrestagency", "Abbreviation" )
                .endAssociation()
                .addAssociation( "chargedwith" )
                    .to( config.getChargedWith() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( "arrestee" )
                    .toEntity( "charge" )
                .entityIdGenerator( row -> Optional.ofNullable( Parsers.getAsString( row.get( "SSN" ) ) ).orElse( "" ) + Optional.ofNullable( Parsers.getAsString( row.get( "Statute/Offense" ) ) ).orElse( "" ) )
                .addProperty( "general.stringid" , "PartyID")
                    .addProperty( "event.ChargeLevel" )
                        .value( ZuercherArrest::chargeLevel ).ok()
                .endAssociation()
                .addAssociation( "appearsin" )
                    .to( config.getAppearsIn() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( "arrestee" )
                    .toEntity( "pretrialcase" )
                    .addProperty( "general.stringid" )
                        .value( row -> Parsers.getAsString( row.getAs( "Arrest Transaction number" )) + Parsers.getAsString( row.getAs( "PartyID" )) ).ok()
                .endAssociation()
                .addAssociation( "livesat" )
                    .to( config.getLivesAt() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( "arrestee" )
                    .toEntity( "address" )
                    .entityIdGenerator( row -> row.get( "PartyID" ) +  getFullAddressAsString( row ) )
                    .addProperty( "general.stringid")
                        .value( ZuercherArrest::getFulladdress ).ok()
                .endAssociation()
                .addAssociation( "hasContact" )
                    .to( config.getContactInformationGiven() )
                    .updateType( UpdateType.Replace )
                    .fromEntity( "arrestee" )
                    .toEntity( "contactInfo" )
                    .addProperty( "ol.id" ).value( ZuercherArrest::formatPhoneNumber ).ok()
                .endAssociation()
                .endAssociations()
                .done();
                //@formatter:on

        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com" );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( arrestsflight, payload );

        missionControl.prepare( flights, false, ImmutableSet.of() ).launch( 10000 );
        MissionControl.succeed();
    }

    public static String formatPhoneNumber( Row row ) {
        String phoneNumber = Parsers.getAsString( row.getAs( "Phone" ) );
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
        String aliases = Parsers.getAsString( row.getAs( "Aliases" ) );
        if ( StringUtils.isNotBlank( aliases ) ) {
            return Lists.newArrayList( aliases.split( "\\|" ) );
        }

        return null;
    }

    public static String getChargeId( Row row ) {
        String caseNumber = Parsers.getAsString( row.getAs( "Arrest Transaction number" ) );
        String personId = Parsers.getAsString( row.getAs( "PartyID" ) );
        String statuteOffense = Parsers.getAsString( row.getAs( "Statute/Offense" ) );
        String chargeNum = Parsers.getAsString( row.getAs( "ChargeNumber" ) );

        return caseNumber + "|" + personId + "|" + statuteOffense + "|" + chargeNum;
    }

    public static List standardRaceList( Row row ) {
        String sr = row.getAs( "Race" );

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
        String eth = row.getAs( "Ethnicity" );

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
        String sex = row.getAs( "Sex" );

        if ( sex != null ) {
            if ( sex.equals( "Male" ) ) {return "M"; }
            if ( sex.equals( "Female" ) ) {return "F"; }
            if ( sex.equals( "" ) ) { return null; }

        }
        return null;

    }

    public static String localStatute( Row row ) {
        String statoff = Parsers.getAsString( row.getAs( "Statute/Offense" ) );
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
        String all = Parsers.getAsString( row.getAs( "Statute/Offense" ) );
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
        String offenseStr = Parsers.getAsString( row.getAs( "Statute/Offense" ) );
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
    //        String person = row.getAs( "SSN" );
    //        String offense = row.getAs( "Statute/Offense" );
    //
    //    }

    public static String getFulladdress( Row row ) {
        String street = row.getAs( "Address" );
        String city = row.getAs( "City" );
        String state = row.getAs( "State" );
        String zipcode = row.getAs( "ZIP" );

        if ( getAddress( street, city, state, zipcode ).isEmpty() ) {
            return "";
        }
        return getAddress( street, city, state, zipcode );
    }

    public static String getFullAddressAsString( Map<String, Object> row ) {
        String street = Optional.ofNullable( Parsers.getAsString( row.get( "Address" ) ) ).orElse( "" );
        String city = Optional.ofNullable( Parsers.getAsString( row.get( "City" ) ) ).orElse( "" );
        String state = Optional.ofNullable( Parsers.getAsString( row.get( "State" ) ) ).orElse( "" );
        String zipcode = Optional.ofNullable( Parsers.getAsString( row.get( "ZIP" ) ) ).orElse( "" );

        return getAddress( street, city, state, zipcode );
    }

    public static String getAddress( String street, String city, String state, String zipcode ) {
        if ( street != null ) {
            StringBuilder address = new StringBuilder( StringUtils.defaultString( street ) );
            address.append( ", " ).append( StringUtils.defaultString( city ) ).append( ", " )
                    .append( StringUtils.defaultString( state ) ).append( " " )
                    .append( StringUtils.defaultString( zipcode ) );

            return address.toString();
        } else if ( city != null ) {
            StringBuilder address = new StringBuilder( StringUtils.defaultString( city ) );
            address.append( ", " ).append( StringUtils.defaultString( state ) ).append( " " )
                    .append( StringUtils.defaultString( zipcode ) );
            return address.toString();
        } else if ( state != null ) {
            StringBuilder address = new StringBuilder( StringUtils.defaultString( state ) );
            address.append( " " ).append( StringUtils.defaultString( zipcode ) );
            return state;
        } else if ( zipcode != null ) {
            return StringUtils.defaultString( zipcode );
        }
        return null;
    }

}

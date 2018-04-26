package com.openlattice.integrations.pennington;

import com.dataloom.mappers.ObjectMappers;
import com.dataloom.streams.StreamUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.Flight;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class ZuercherArrest {
    private static final Logger                      logger      = LoggerFactory.getLogger( ZuercherArrest.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    private static final DateTimeHelper bdHelper = new DateTimeHelper( TimeZones.America_Denver,
            "MM/dd/YY" );
    private static final DateTimeHelper dtHelper = new DateTimeHelper( TimeZones.America_Denver,
            "MM/dd/YY HH:mm" );

//    private static final Pattern    statuteMatcher = Pattern.compile( "([0-9+]\s\-\s(.+)\s(\((.*?)\))" ); //start with a number followed by anything, even empty string. after dash, at least 1 char, 1 whitespace, 2 parentheses
                                                                                                 // with anything (even nothing) in between them

    public static void main( String[] args ) throws InterruptedException, IOException {

        final String jwtToken = args[ 0 ];
        final String arrestsPath = args [ 1 ];
        final String csvFlag = args [ 2 ];


        Payload payload;

        if ( csvFlag.equals( "json" )) {
            Map<String, List<Map<String, String>>> jsonMap = ObjectMappers.getJsonMapper()
                    .readValue( new URL( arrestsPath ), new TypeReference<Map<String, List<Map<String, String>>>>() {
                    } );
            payload = new SimplePayload( StreamUtil.stream( jsonMap.get( "data" ) ) );
        }
        else payload = new SimplePayload( arrestsPath );

//        SimplePayload payload = new SimplePayload( arrestsPath );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight arrestsflight = Flight.newFlight()
                .createEntities()

                .addEntity( "arrestee")
                    .to( "PenZuercherArrestee" )
                    .entityIdGenerator( row -> row.get( "SSN") )
                    .addProperty( "nc.PersonSurName", "Last Name" )
                    .addProperty( "nc.PersonGivenName", "First Name" )
                    .addProperty( "nc.PersonMiddleName", "Middle Name" )
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.PersonRace" )
                        .value( ZuercherArrest::standardRaceList).ok()
                    .addProperty( "nc.PersonSex")
                        .value( ZuercherArrest::standardSex ).ok()
                    .addProperty( "nc.PersonEthnicity" )
                        .value( ZuercherArrest::standardEthnicity).ok()
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> bdHelper.parseDate( "DOB" ) ).ok()
                .endEntity()
                .addEntity( "incident" )
                    .to( "PenZuercherIncident")
                    .addProperty( "ol.gangactivity", "Other Gang" )
                    .addProperty( "ol.juvenilegang", "Juvenile Gang" )
                    .addProperty( "ol.datetime_start" )
                        .value( row ->  dtHelper.parse( "Incident Start Date/Time" )).ok()
                    .addProperty( "ol.datetime_reported" )
                        .value( row -> dtHelper.parse( "Reported Date/Time" ) ).ok()
                    .addProperty( "publicsafety.drugspresent", "Offender(s) Used Drugs in Crime" )
                    .addProperty( "ol.alcoholincrime", "Offender(s) Used Alcohol in Crime" )
                    .addProperty( "ol.computerincrime", "Offender(s) Used Computer Equipment" )
                .endEntity()
                .addEntity( "offense" )
                    .to( "PenZuercherOffense" )
                    .addProperty( "criminaljustice.offenseid" )
                        .value( ZuercherArrest::localStatute ).ok()
                    .addProperty( "criminaljustice.localstatute")
                        .value( ZuercherArrest::localStatute ).ok()
                    .addProperty( "event.OffenseLocalDescription")
                        .value( ZuercherArrest::offense ).ok()
                    .addProperty( "event.comments", "Offense Details" )
                    .addProperty( "ol.numberofcounts", "Offense Count" )
                .endEntity()
                .addEntity( "courtcase" )
                    .to( "PenZuercherCase" )
                    .addProperty( "j.CaseNumberText", "Case Number" )
                .endEntity()
                .addEntity( "address" )
                    .to( "PenZuercherAddress")
                    .addProperty( "location.Address")
                        .value( ZuercherArrest::getFulladdress ).ok()
                    .addProperty( "location.city", "City" )
                    .addProperty( "location.state", "State" )
                    .addProperty( "location.zip", "ZIP")
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "arrestedin" )
                    .to( "PenZuercherArrests" )
                    .fromEntity( "arrestee" )
                    .toEntity( "incident" )
                    .addProperty( "ol.arrestdatetime" )
                        .value( row -> dtHelper.parse( "Arrest Date/Time" ) ).ok()
                    .addProperty( "nc.SubjectIdentification", "SSN" )
                    .addProperty( "person.ageatevent", "Age When Arrested" )
                .endAssociation()
                .addAssociation( "chargedwith" )
                    .to("PenZuercherchargedwith")
                    .fromEntity( "arrestee" )
                    .toEntity( "offense" )
                    .addProperty( "general.stringid" , "SSN")
                    .addProperty( "event.ChargeLevel" )
                        .value( ZuercherArrest::chargeLevel ).ok()
                .endAssociation()
                .addAssociation( "leadsto" )
                    .to( "PenZuercherLeadsto")
                    .fromEntity( "offense" )
                    .toEntity( "courtcase" )
                    .entityIdGenerator( row -> row.get( "Statute/Offense" ) + row.get( "Last Name" ) + row.get("First Name")
                                        + row.get( "Middle Name" ) + row.get( "Case Number" ))
                    .addProperty( "general.stringid", "Case Number" )
                .endAssociation()
                .addAssociation( "locatedat" )
                    .to("PenZLocatedAt")
                    .fromEntity( "arrestee" )
                    .toEntity( "address" )
                    .addProperty( "location.address")
                        .value( ZuercherArrest::getFulladdress ).ok()
                    .addProperty( "nc.SubjectIdentification", "SSN" )
                .endAssociation()
                .endAssociations()
                .done();
                //@formatter:on

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload>  flights = new HashMap<>( 1 );
        flights.put( arrestsflight, payload );

        shuttle.launchPayloadFlight( flights );

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

    public static String localStatute( Row row ){
        String statoff = Parsers.getAsString (row.getAs( "Statute/Offense" ));
        if (statoff != null) {
            String [] statutesplit = statoff.split( " ");
            String statute = statutesplit[ 0 ];                //SAVE 1ST ELEMENT IN ARRAY AS A STRING
            if (Character.isDigit( statute.charAt(0)) ) {     //if it begins with a number, assume it's the statute #
                return statute;
            }
            return null;
        }
        return null;
    }


    public static String chargeLevel( Row row ) {
        String all = Parsers.getAsString (row.getAs( "Statute/Offense" )).trim();
        if (StringUtils.isNotBlank( all )){
            String [] splitlevel = all.split( " " );
            String charge = splitlevel[splitlevel.length-1];
            charge = charge.replace( "(","" );
            charge = charge.replace( ")", "" );

            if (charge.startsWith( "M" ) | charge.startsWith( "F" )){
                return charge.substring( charge.length() - 2 );    //return the last 2 characters. For cases where the last element in the array ins now, "FamilyM1", was "Family(M1)"
            }
            return null;
        }
        return null;
    }

    //split on string, get an array.
    // string 0 is statute, string 1 goes away. String length-1 is degree level, string 2-length-2 is the middle. Join on spaces (put back as string)
    public static String offense( Row row ){
        String all = Parsers.getAsString (row.getAs( "Statute/Offense" )).trim();
        if (StringUtils.isNotBlank( all )) {

                //if it begins with a number, assume it's the statute #
                if (Character.isDigit( all.charAt( 0 ) )) {
//                String offense = Arrays.toString( splitall );    //convert array to string
                all.replaceAll( "^[^a-zA-Z]", "" ).trim();  //remove all non-alphanumeric characters from front. Regex replaces anything except a-z, A-Z

                    //If there is a charge at the end
                    if (all.endsWith( ")" )) {
                    String offense = all.substring( 0, all.length() - 4 );     //removes last 4 characters, i.e. (f1)
                    return offense;
                }

                    //if there is no charge level at the end
                    return all;
            }
                    //If no statute exists: same as above, but do not remove numbers from beginning
//                    String offense = Arrays.toString( splitall );
                String offense = all.replaceAll( "^[^a-zA-Z\\-]", "" ).trim();
                return offense;
        }
        //if there is only 1 element in the array
//        return all;}
        return null;
    }

    public static String getFulladdress( Row row ) {
        String street = row.getAs( "Address" );
        String city = row.getAs( "City" );
        String state = row.getAs( "State" );
        String zipcode = row.getAs( "ZIP" );

        if ( street != null ) {
            StringBuilder address = new StringBuilder( StringUtils.defaultString( street ) );
            address.append( ", " ).append( StringUtils.defaultString( city ) ).append( ", " )
                    .append( StringUtils.defaultString( state ) ).append( " " )
                    .append( StringUtils.defaultString( zipcode ) );

            return address.toString();
        }

        else if ( city != null ) {
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

    //            ArrayList<String> splitall = new ArrayList<>( Arrays.asList( all.split(" ")) );
    //                splitall.remove[ 0 ];      //removes first element, i.e. 70010
    //
    //           String [] checkStatuteExists = splitall.toArray(new String [splitall.size()] );  //save arraylist as array


    //then save arraylist as a string
    //                    StringBuilder sb = new StringBuilder ();
    //                    for (String s : splitall)
    //                    {
    //                        sb.append(s);
    ////                        sb.append( "\t" );
    //                        return sb.toString();
    //                    }






}

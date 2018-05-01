package com.openlattice.integrations.pennington;

import com.openlattice.client.RetrofitFactory;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class MinPennHearings {
    private static final Logger                      logger      = LoggerFactory.getLogger( MinPennHearings.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    private static final DateTimeHelper dtHelper = new DateTimeHelper( TimeZones.America_Chicago,
            "MM/dd/yyyy" );
    private static final DateTimeHelper tHelper = new DateTimeHelper( TimeZones.America_Chicago,
            "hh:mma" );


    public static void main( String[] args ) throws InterruptedException, IOException {

        final String jwtToken = args[ 0 ];
        final String hearingsPath = args[ 1 ];

        SimplePayload payload = new SimplePayload( hearingsPath );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight hearingsflight = Flight.newFlight()
                .createEntities()

                .addEntity( "inmate" )
                    .to( "southdakotapeople" )              //NEED NC SUBJECTID
                    .entityIdGenerator( row -> row.get( "InmateName" ) + row.get( "InmateDOB" ) )
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> dtHelper.parseDate( row.getAs("InmateDOB") )).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "InmateName" )) ).ok()
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName (row.getAs( "InmateName" ))).ok()
                    .addProperty( "nc.PersonMiddleName" )
                        .value(  row -> getMiddleName (row.getAs( "InmateName" ))).ok()
                    .addProperty( "nc.PersonSuffix" )
                        .value( MinPennHearings::getSuffix ).ok()
                .endEntity()
                .addEntity( "officerperson" )
                    .to( "MinPenPeople" )
                    .entityIdGenerator( row -> row.get("JudicialOfficer") )
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName (row.getAs( "JudicialOfficer" ))).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "JudicialOfficer" ) ) ).ok()
                    .addProperty( "nc.PersonMiddleName" )
                        .value(  row -> getMiddleName (row.getAs( "JudicialOfficer" ))).ok()
                .endEntity()
                .addEntity( "officer" )
                    .to("MinPennOfficers")
                    .entityIdGenerator( row -> row.get("JudicialOfficer") )
                    .addProperty( "publicsafety.personneltitle" )
                        .value( row -> "Judicial Officer" ).ok()
                    .endEntity()
                .addEntity( "hearing" )        //As justice.case right now
                    .to("MinPennHearings")
                    .addProperty( "j.CaseNumberText", "ID" )
                    .addProperty( "general.date").value( row -> dtHelper.parseDate( row.getAs( "HearingDate" ) ) ).ok()
                    .addProperty( "justice.courtcasetype", "HearingType" )
                    .addProperty( "date.timeOfDay" ).value( row -> tHelper.parseTime( row.getAs( "HearingTime" ) )).ok()
                    .addProperty( "event.comments", "HearingComment" )
                    .addProperty( "ol.update", "UpdateType" )
                .endEntity()
                .addEntity( "case" )
                    .to( "MinPennCase")
                    .addProperty( "j.CaseNumberText", "DocketNumber" )
                .endEntity()
                .addEntity( "courtroom" )      //using geo.address
                    .to( "MinPennCourtroom" )
                    .addProperty( "location.Address", "Courtroom" )
                    .addProperty( "location.name", "Courtroom" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "appearsin" )
                    .to( "MinPennAppearsin" )
                    .fromEntity( "inmate" )
                    .toEntity( "hearing" )
                    .entityIdGenerator( row -> row.get( "ID" ) + row.get ("InmateName") )
                    .addProperty( "general.stringid", "ID")
                    .addProperty( "nc.SubjectIdentification", "InmateName" )
                .endAssociation()
                .addAssociation( "becomes" )
                    .to("MinPennBecomes")
                    .fromEntity( "officerperson" )
                    .toEntity( "officer" )
                    .addProperty( "nc.SubjectIdentification", "JudicialOfficer" )
                .endAssociation()
                .addAssociation( "appearsin2" )
                    .to( "MinPennAppearsin" )
                    .fromEntity( "officerperson" )
                    .toEntity( "hearing" )
                    .entityIdGenerator( row -> row.get( "ID" ) + row.get ("JudicialOfficer") )
                    .addProperty( "general.stringid", "ID" )
                    .addProperty( "nc.SubjectIdentification", "JudicialOfficer" )
                .endAssociation()
                .addAssociation( "appearsin3" )
                    .to( "MinPennAppearsin" )
                    .fromEntity( "hearing" )
                    .toEntity( "case" )
                    .addProperty( "general.stringid" )
                        .value( row -> Parsers.getAsString( row.getAs( "DocketNumber" ) ) + Parsers.getAsString( row.getAs( "ID" ) ) ).ok()
                .endAssociation()
                .addAssociation( "occurredat" )
                    .to( "MinPennOccurredat" )
                    .fromEntity( "hearing" )
                    .toEntity( "courtroom" )
                    .addProperty( "general.stringid", "ID" )
                .endAssociation()

                .endAssociations()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload>  flights = new HashMap<>( 1 );
        flights.put( hearingsflight, payload );

        shuttle.launchPayloadFlight( flights );

}

    public static String getLastName (Object obj){
        String all = Parsers.getAsString( obj );
        if (all == null) return null;
        all = all.trim();
        if (StringUtils.isNotBlank( all )){
            String [] namesplit = all.split( "," );
            String lastname = namesplit[ 0 ].trim();   //1st element in array is always the last name
                    return lastname;
        }
        return null;
    }

//    public static String getLastNameJudge (Row row){
//        String all = Parsers.getAsString(row.getAs( "JudicialOfficer" )).trim();
//        if (StringUtils.isNotBlank( all )){
//            String [] namesplit = all.split( "," );
//            String lastname = namesplit[ 0 ];   //1st element in array is always the last name
//            return lastname;
//        }
//        return null;
//    }


    public static String getFirstName (Object obj) {
        String all = Parsers.getAsString( obj );
        if (all == null) return null;
        all = all.trim();

        if ( StringUtils.isNotBlank( all ) ) {
            String[] namesplit = all.split( "," );
            String firstname = namesplit[ 1 ].trim();      //2nd element in array is always firstname

            String[] firstname2 = firstname.split( " " );

            if ( firstname2.length > 1 ) {  //if 2 words, there is a middle name
                return firstname2[ 0 ] ;
            }
            return firstname;
        }
        return null;
    }


    public static String getMiddleName (Object obj) {
        String all = Parsers.getAsString( obj );
        if (all == null) return null;
        all = all.trim();
        if (StringUtils.isNotBlank( all )){
            String [] namesplit = all.split( "," );

            String firstmid = namesplit[ 1 ];      //2nd element in array is always first/middle name
            firstmid = firstmid.trim();
            String [] firstmid2 = firstmid.split( " " );


            if (firstmid2.length > 1) {  //if 2 words, last one 1 is the middle name
                String middle = firstmid2[firstmid2.length-1];
                return middle;
            }
            return null;
        }
        return null;
    }

    public static String getSuffix (Row row){
        String all = Parsers.getAsString(row.getAs( "InmateName" )).trim();
        if (StringUtils.isNotBlank( all )){
            String [] namesplit = all.split( "," );

            if ( namesplit.length == 3 ) {
                String suffix = namesplit[namesplit.length-1 ];   //return last element
                return suffix;
            }
            return null;
        }
        return null;
    }

}

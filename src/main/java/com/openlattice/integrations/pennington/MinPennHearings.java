package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableSet;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class MinPennHearings {
    private static final Logger                      logger      = LoggerFactory.getLogger( MinPennHearings.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;

    private static final String             dateTimePattern = "MM/dd/yyyy hh:mma";
    private static final JavaDateTimeHelper pennDTHelper    = new JavaDateTimeHelper( TimeZones.America_Denver,
            dateTimePattern );
    private static final JavaDateTimeHelper minnDTHelper    = new JavaDateTimeHelper( TimeZones.America_Chicago,
            dateTimePattern );

    private static final JavaDateTimeHelper bdHelper = new JavaDateTimeHelper( TimeZones.America_Denver, "MM/dd/yyyy" );

    private static final String CASE_ALIAS    = "case";
    private static final String HEARING_ALIAS = "hearing";
    private static final String JUDGE_ALIAS   = "judge";
    private static final String PERSON_ALIAS  = "person";

    private static final String CASE_ENTITY_SET       = "southdakotapretrialcaseprocessings";
    private static final String HEARING_ENTITY_SET    = "southdakotahearings";
    private static final String JUDGE_ENTITY_SET      = "southdakotajudges";
    private static final String APPEARS_IN_ENTITY_SET = "southdakotaappearsin";
    private static final String OVERSEES_ENTITY_SET   = "southdakotaoversees";
    private static final String PEOPLE_ENTITY_SET     = "southdakotapeople";

    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String hearingsPath = args[ 2 ];
        SimplePayload payload = new SimplePayload( hearingsPath );
        String jwtToken = MissionControl.getIdToken( username, password );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight hearingsflight = Flight.newFlight()
                .createEntities()
                .addEntity( JUDGE_ALIAS )
                    .to( JUDGE_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get("JudicialOfficer") ) )
                    .addProperty( "nc.PersonGivenName" )
                        .value( row -> getFirstName (row.getAs( "JudicialOfficer" ))).ok()
                    .addProperty( "nc.PersonSurName" )
                        .value( row -> getLastName( row.getAs( "JudicialOfficer" ) ) ).ok()
                .endEntity()
                .addEntity( HEARING_ALIAS )
                    .to( HEARING_ENTITY_SET )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get( "ID" ) ) )
                    .addProperty( "j.CaseNumberText", "ID" )
                    .addProperty( "justice.courtcasetype", "HearingType" )
                    .addProperty( "general.datetime" ).value( MinPennHearings::getDateTimeFromRow ).ok()
                    .addProperty( "event.comments", "HearingComment" )
                    .addProperty( "ol.update", "UpdateType" )
                    .addProperty( "justice.courtroom", "Courtroom" )
                .endEntity()
                .addEntity( CASE_ALIAS )
                    .to( CASE_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get("DocketNumber" ) ) )
                    .addProperty( "j.CaseNumberText", "DocketNumber" )
                .endEntity()
                .addEntity( PERSON_ALIAS )
                    .to( PEOPLE_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .addProperty( "nc.SubjectIdentification", "PartyID" )
                    .addProperty( "nc.PersonGivenName", "InmateFName" )
                    .addProperty( "nc.PersonSurName", "InmateLName" )
                    .addProperty( "nc.PersonMiddleName", "InmateMidName" )
                    .addProperty( "nc.PersonSuffix", "InmateSfxName" )
                    .addProperty( "nc.PersonBirthDate" ).value( row -> bdHelper.parseDate( row.getAs( "InmateDOB" ) ) ).ok()
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "overseescase" )
                    .updateType( UpdateType.Replace )
                    .to( OVERSEES_ENTITY_SET )
                    .fromEntity( JUDGE_ALIAS )
                    .toEntity( CASE_ALIAS )
                    .addProperty( "date.completeddatetime" ).value( MinPennHearings::getDateTimeFromRow ).ok()
                .endAssociation()
                .addAssociation( "overseeshearing" )
                    .updateType( UpdateType.Replace )
                    .to( OVERSEES_ENTITY_SET )
                    .fromEntity( JUDGE_ALIAS )
                    .toEntity( HEARING_ALIAS )
                    .addProperty( "date.completeddatetime" ).value( MinPennHearings::getDateTimeFromRow ).ok()
                .endAssociation()
                .addAssociation( "appearsin" )
                    .updateType( UpdateType.Replace )
                    .to( APPEARS_IN_ENTITY_SET )
                    .fromEntity( HEARING_ALIAS )
                    .toEntity( CASE_ALIAS )
                    .addProperty( "general.stringid" )
                        .value( row -> Parsers.getAsString( row.getAs( "DocketNumber" ) ) + Parsers.getAsString( row.getAs( "ID" ) ) ).ok()
                .endAssociation()
                .addAssociation( "appearsInHearing" )
                    .updateType( UpdateType.Replace )
                    .to( APPEARS_IN_ENTITY_SET )
                    .fromEntity( PERSON_ALIAS )
                    .toEntity( HEARING_ALIAS )
                    .addProperty( "general.stringid" ).value( row -> Parsers.getAsString( row.getAs( "ID" ) ) + "|" + Parsers.getAsString( row.getAs( "PartyID" ) ) ).ok()
                .endAssociation()

                .endAssociations()
                .done();

        MissionControl missionControl = new MissionControl( environment, () -> jwtToken, "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com" );
        Map<Flight, Payload>  flights = new HashMap<>( 1 );
        flights.put( hearingsflight, payload );

        missionControl.prepare( flights, false, ImmutableSet.of() ).launch();

    }

    private static Object getDateTimeFromRow( Row row ) {
        String caseNum = Parsers.getAsString( row.getAs( "DocketNumber" ) );

        String dateStr = Parsers.getAsString( row.getAs( "HearingDate" ) );
        String timeStr = Parsers.getAsString( row.getAs( "HearingTime" ) );
        if ( caseNum == null || dateStr == null || timeStr == null ) {
            logger.debug( "Unable to get datetime." );
            return null;
        }

        String dateTimeStr = dateStr.trim() + " " + timeStr.trim();
        return (caseNum.trim().startsWith( "49" ) ? minnDTHelper : pennDTHelper).parseDateTime( dateTimeStr );
    }

    public static String getLastName (Object obj){
        String nameStr = Parsers.getAsString( obj );
        if ( StringUtils.isBlank( nameStr ) )
            return null;
        return nameStr.trim().split( "," )[ 0 ];
    }


    public static String getFirstName (Object obj) {
        String nameStr = Parsers.getAsString( obj );
        if ( StringUtils.isBlank( nameStr ) )
            return null;

        String[] namesplit = nameStr.trim().split( "," );

        if ( namesplit.length > 1 ) {
            return namesplit[ 1 ].trim().split( " " )[ 0 ];
        }
        return null;
    }


}

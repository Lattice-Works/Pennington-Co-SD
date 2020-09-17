package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.UpdateType;
import com.openlattice.integrations.pennington.utils.EdmConstants;
import com.openlattice.integrations.pennington.utils.IntegrationAliases;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.MissionParameters;
import com.openlattice.shuttle.Row;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.CsvPayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class MinPennHearings {
    private static final Logger                      logger      = LoggerFactory.getLogger( MinPennHearings.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PROD_INTEGRATION;

    private static final String             dateTimePattern = "MM/dd/yyyy hh:mma";
    private static final String[]           dateTimePatterns = new String[] { dateTimePattern };

    private static final String             datePattern = "MM/dd/yyyy";
    private static final String[]           datePatterns = new String[] { datePattern };

    private static final JavaDateTimeHelper pennDTHelper    = new JavaDateTimeHelper(
            TimeZones.America_Denver,
            dateTimePatterns,
            true
    );
    private static final JavaDateTimeHelper minnDTHelper    = new JavaDateTimeHelper(
            TimeZones.America_Chicago,
            dateTimePatterns,
            true
    );

    private static final JavaDateTimeHelper bdHelper = new JavaDateTimeHelper(
            TimeZones.America_Denver,
            datePatterns,
            true
    );

    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String hearingsPath = args[ 2 ];
        CsvPayload payload = new CsvPayload( hearingsPath );
        String jwtToken = MissionControl.getIdToken( username, password );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //@formatter:off
        Flight hearingsflight = Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.JUDGE_ALIAS )
                    .to( EdmConstants.JUDGE_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .addProperty( EdmConstants.PERSON_ID_FQN, IntegrationAliases.JUDGE_ID_COL )
                    .addProperty( EdmConstants.FIRST_NAME_FQN )
                        .value( row -> getFirstName (row.getAs( IntegrationAliases.JUDICIAL_OFFICER_COL ))).ok()
                    .addProperty( EdmConstants.LAST_NAME_FQN )
                        .value( row -> getLastName( row.getAs( IntegrationAliases.JUDICIAL_OFFICER_COL ) ) ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.HEARING_ALIAS )
                    .to( EdmConstants.HEARING_ENTITY_SET )
                    .updateType( UpdateType.Replace )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get( IntegrationAliases.GENERAL_ID_COL ) ) )
                    .addProperty( EdmConstants.CASE_NO_FQN, IntegrationAliases.GENERAL_ID_COL )
                    .addProperty( EdmConstants.CASE_TYPE_FQN, IntegrationAliases.HEARING_TYPE_COL )
                    .addProperty( EdmConstants.COURT_DOCKET_FQN, IntegrationAliases.DOCKET_NO_COL )
                    .addProperty( EdmConstants.DATETIME_FQN ).value( MinPennHearings::getDateTimeFromRow ).ok()
                    .addProperty( EdmConstants.COMMENTS_FQN, IntegrationAliases.HEARING_COMMENT_COL )
                    .addProperty( EdmConstants.UPDATE_FQN, IntegrationAliases.UPDATE_TYPE_COL )
                    .addProperty( EdmConstants.COURTROOM_FQN, IntegrationAliases.COURTROOM_COL )
                    .addProperty( EdmConstants.INACTIVE_FQN ).value( MinPennHearings::hearingIsCancelled ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.CASE_ALIAS )
                    .to( EdmConstants.CASES_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .entityIdGenerator( row -> Parsers.getAsString( row.get( IntegrationAliases.DOCKET_NO_COL ) ) )
                    .addProperty( EdmConstants.CASE_NO_FQN, IntegrationAliases.DOCKET_NO_COL )
                .endEntity()
                .addEntity( IntegrationAliases.COURTHOUSE_ALIAS )
                    .to( EdmConstants.COURTHOUSES_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .addProperty( EdmConstants.GENERAL_ID_FQN ).value( MinPennHearings::getCountyPrefix ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.COUNTY_ALIAS )
                    .to( EdmConstants.COUNTIES_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .addProperty( EdmConstants.GENERAL_ID_FQN ).value( MinPennHearings::getCountyPrefix ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.COURTROOM_ALIAS )
                    .to( EdmConstants.COURTROOMS_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .addProperty( EdmConstants.OL_ID_FQN ).value( MinPennHearings::getCourtroomId ).ok()
                    .addProperty( EdmConstants.ROOM_NO_FQN, IntegrationAliases.COURTROOM_ALIAS )
                .endEntity()
                .addEntity( IntegrationAliases.PERSON_ALIAS )
                    .to( EdmConstants.PEOPLE_ENTITY_SET )
                    .updateType( UpdateType.Merge )
                    .addProperty( EdmConstants.PERSON_ID_FQN, IntegrationAliases.PERSON_ID_COL )
                    .addProperty( EdmConstants.FIRST_NAME_FQN, IntegrationAliases.INMATE_FIRST_NAME_COL )
                    .addProperty( EdmConstants.LAST_NAME_FQN, IntegrationAliases.INMATE_LAST_NAME_COL )
                    .addProperty( EdmConstants.MIDDLE_NAME_FQN, IntegrationAliases.INMATE_MIDDLE_NAME_COL )
                    .addProperty( EdmConstants.SUFFIX_FQN, IntegrationAliases.INMATE_SUFFIX_NAME_COL )
                    .addProperty( EdmConstants.DOB_FQN ).value( row -> bdHelper.parseDate( row.getAs( IntegrationAliases.INMATE_DOB_COL ) ) ).ok()
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( IntegrationAliases.OVERSEES_CASE_ALIAS )
                    .updateType( UpdateType.Replace )
                    .to( EdmConstants.OVERSEES_ENTITY_SET )
                    .fromEntity( IntegrationAliases.JUDGE_ALIAS )
                    .toEntity( IntegrationAliases.CASE_ALIAS )
                    .addProperty( EdmConstants.COMPLETED_DATETIME_FQN ).value( MinPennHearings::getDateTimeFromRow ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.OVERSEES_HEARING_ALIAS )
                    .updateType( UpdateType.Replace )
                    .to( EdmConstants.OVERSEES_ENTITY_SET )
                    .fromEntity( IntegrationAliases.JUDGE_ALIAS )
                    .toEntity( IntegrationAliases.HEARING_ALIAS )
                    .addProperty( EdmConstants.COMPLETED_DATETIME_FQN ).value( MinPennHearings::getDateTimeFromRow ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                    .updateType( UpdateType.Replace )
                    .to( EdmConstants.APPEARS_IN_ENTITY_SET )
                    .fromEntity( IntegrationAliases.HEARING_ALIAS )
                    .toEntity( IntegrationAliases.CASE_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN )
                        .value( row -> Parsers.getAsString( row.getAs( IntegrationAliases.DOCKET_NO_COL ) ) + Parsers.getAsString( row.getAs( IntegrationAliases.GENERAL_ID_COL ) ) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.APPEARS_IN_HEARING_ALIAS )
                    .updateType( UpdateType.Replace )
                    .to( EdmConstants.APPEARS_IN_ENTITY_SET )
                    .fromEntity( IntegrationAliases.PERSON_ALIAS )
                    .toEntity( IntegrationAliases.HEARING_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> Parsers.getAsString( row.getAs( IntegrationAliases.GENERAL_ID_COL ) ) + "|" + Parsers.getAsString( row.getAs( IntegrationAliases.PERSON_ID_COL ) ) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.HEARING_APPEARS_IN_COUNTY_ALIAS )
                    .updateType( UpdateType.Replace )
                    .to( EdmConstants.APPEARS_IN_ENTITY_SET )
                    .fromEntity( IntegrationAliases.HEARING_ALIAS )
                    .toEntity( IntegrationAliases.COUNTY_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> getCountyPrefix( row ) + "|" + row.getAs( IntegrationAliases.GENERAL_ID_COL ) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.HEARING_APPEARS_IN_COURTROOM_ALIAS )
                    .updateType( UpdateType.Replace )
                    .to( EdmConstants.APPEARS_IN_ENTITY_SET )
                    .fromEntity( IntegrationAliases.HEARING_ALIAS )
                    .toEntity( IntegrationAliases.COURTROOM_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> row.getAs( IntegrationAliases.GENERAL_ID_COL ) + "|" + getCourtroomId( row ) ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.HEARING_APPEARS_IN_COURTHOUSE_ALIAS )
                    .updateType( UpdateType.Replace )
                    .to( EdmConstants.APPEARS_IN_ENTITY_SET )
                    .fromEntity( IntegrationAliases.HEARING_ALIAS )
                    .toEntity( IntegrationAliases.COURTROOM_ALIAS )
                    .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> row.getAs( IntegrationAliases.GENERAL_ID_COL ) + "|" + getCountyPrefix( row ) ).ok()
                .endAssociation()

                .endAssociations()
                .done();

        MissionControl missionControl = new MissionControl( environment, () -> jwtToken, "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com", MissionParameters.empty() );
        Map<Flight, Payload>  flights = new HashMap<>( 1 );
        flights.put( hearingsflight, payload );
        missionControl.prepare( flights, false, ImmutableMap.of(), ImmutableSet.of() ).launch( 10000 );
        MissionControl.succeed();

    }

    private static boolean hearingIsCancelled( Row row ) {
        String updateType = Parsers.getAsString( row.getAs( IntegrationAliases.UPDATE_TYPE_COL ) );
        return StringUtils.isNotBlank( updateType ) && updateType.toLowerCase().trim().equals( "cancelled" );
    }

    private static String getCountyPrefix( Row row ) {
        String caseNum = Parsers.getAsString( row.getAs( IntegrationAliases.DOCKET_NO_COL ) );
        if (StringUtils.isNoneBlank( caseNum )) {
            return caseNum.trim().substring( 0, 2 );
        }

        return null;
    }

    private static String getCourtroomId( Row row ) {
        String countyPrefix = getCountyPrefix( row );
        String courtroom = row.getAs( IntegrationAliases.COURTROOM_COL );
        return countyPrefix + "|" + courtroom;
    }

    private static Object getDateTimeFromRow( Row row ) {
        String countyPrefix = getCountyPrefix( row );

        String dateStr = Parsers.getAsString( row.getAs( IntegrationAliases.HEARING_DATE_COL ) );
        String timeStr = Parsers.getAsString( row.getAs( IntegrationAliases.HEARING_TIME_COL ) );
        if ( countyPrefix == null || dateStr == null || timeStr == null ) {
            logger.debug( "Unable to get datetime." );
            return null;
        }

        String dateTimeStr = dateStr.trim() + " " + timeStr.trim();
        return (countyPrefix.equals( "49" ) ? minnDTHelper : pennDTHelper).parseDateTime( dateTimeStr );
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

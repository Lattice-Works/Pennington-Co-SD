package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

public class OdysseyCasesDailyDump {

    protected static final Logger logger = LoggerFactory.getLogger( OdysseyCasesDailyDump.class );

    public static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;

    private static final DateTimeHelper bdHelper = new DateTimeHelper( DateTimeZone.forOffsetHours( -6 ),
            "yyyy-MM-dd HH:mm:ss" );

    private static final String PERSON_ENTITY_SET_NAME     = "southdakotapeople";
    private static final String CASES_ENTITY_SET_NAME      = "southdakotapretrialcaseprocessings";
    private static final String ADDRESSES_ENTITY_SET_NAME  = "southdakotaaddresses";
    private static final String APPEARS_IN_ENTITY_SET_NAME = "southdakotaappearsin";
    private static final String LIVES_AT_ENTITY_SET_NAME   = "southdakotalivesat";

    private static final String PERSON_ID_FQN   = "nc.SubjectIdentification";
    private static final String FIRST_NAME_FQN  = "nc.PersonGivenName";
    private static final String LAST_NAME_FQN   = "nc.PersonSurName";
    private static final String MIDDLE_NAME_FQN = "nc.PersonMiddleName";
    private static final String SUFFIX_FQN      = "nc.PersonSuffix";
    private static final String DOB_FQN         = "nc.PersonBirthDate";
    private static final String RACE_FQN        = "nc.PersonRace";
    private static final String ETHNICITY_FQN   = "nc.PersonEthnicity";
    private static final String EYE_FQN         = "nc.PersonEyeColorText";
    private static final String HEIGHT_FQN      = "nc.PersonHeightMeasure";
    private static final String WEIGHT_FQN      = "nc.PersonWeightMeasure";
    private static final String GENDER_FQN      = "nc.PersonSex";
    private static final String CASE_NO_FQN     = "j.CaseNumberText";
    private static final String ADDRESS_FQN     = "location.Address";
    private static final String STRING_ID_FQN   = "general.stringid";
    private static final String DATETIME_FQN    = "general.datetime";

    private static final String PERSON_ID_COL   = "PartyID";
    private static final String NAME_COL        = "Name";
    private static final String FIRST_NAME_COL  = "NameFirst";
    private static final String MIDDLE_NAME_COL = "NameMid";
    private static final String LAST_NAME_COL   = "NameLast";
    private static final String SUFFIX_NAME_COL = "NameSfxKy";
    private static final String DOB_COL         = "DtDOB";
    private static final String ADDRESS_COL     = "Address";
    private static final String RACE_COL        = "RaceKy";
    private static final String ETHNICITY_COL   = "EthnicKy";
    private static final String EYE_COL         = "EyeKy";
    private static final String HEIGHT_COL      = "HtInches";
    private static final String WEIGHT_COL      = "WtLbs";
    private static final String GENDER_COL      = "GenderKy";
    private static final String CASE_NO_COL     = "CaseNbr";
    private static final String FELONY_COL      = "FelonyM1Cntr";
    private static final String SEALED_COL      = "SealedCntr";

    private static final String PERSON_ALIAS  = "person";
    private static final String CASE_ALIAS    = "case";
    private static final String ADDRESS_ALIAS = "address";

    private static final SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String filePath = args[ 2 ];

        final String jwtToken = MissionControl.getIdToken( username, password );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Map<Flight, Payload> flights = Maps.newHashMap();
        Payload payload = new SimplePayload( filePath );

        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( PERSON_ALIAS )
                .to( PERSON_ENTITY_SET_NAME )
                .addProperty( PERSON_ID_FQN, PERSON_ID_COL )
                .addProperty( FIRST_NAME_FQN, FIRST_NAME_COL )
                .addProperty( MIDDLE_NAME_FQN, MIDDLE_NAME_COL )
                .addProperty( LAST_NAME_FQN, LAST_NAME_COL )
                .addProperty( SUFFIX_FQN, SUFFIX_NAME_COL )
                .addProperty( DOB_FQN ).value( row -> bdHelper.parseDate( row.getAs( DOB_COL ) ) ).ok()
                .addProperty( RACE_FQN, RACE_COL )
                .addProperty( ETHNICITY_FQN, ETHNICITY_COL )
                .addProperty( EYE_FQN, EYE_COL )
                .addProperty( GENDER_FQN, GENDER_COL )
                .addProperty( HEIGHT_FQN ).value( row -> Parsers.parseInt( row.getAs( HEIGHT_COL ) ) ).ok()
                .addProperty( WEIGHT_FQN ).value( row -> Parsers.parseInt( row.getAs( WEIGHT_COL ) ) ).ok()
                .endEntity()

                .addEntity( CASE_ALIAS )
                .to( CASES_ENTITY_SET_NAME )
                .entityIdGenerator( row -> row.get( CASE_NO_COL ).toString() )
                .addProperty( CASE_NO_FQN, CASE_NO_COL )
                .endEntity()

                .addEntity( ADDRESS_ALIAS )
                .to( ADDRESSES_ENTITY_SET_NAME )
                .addProperty( ADDRESS_FQN, ADDRESS_COL )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "appearsin" )
                .to( APPEARS_IN_ENTITY_SET_NAME )
                .fromEntity( PERSON_ALIAS )
                .toEntity( CASE_ALIAS )
                .addProperty( STRING_ID_FQN, CASE_NO_COL )
                .addProperty( DATETIME_FQN ).value( row -> bdHelper.parse( format.format( new Date() ) ) ).ok()
                .endAssociation()

                .addAssociation( "livesat" )
                .to( LIVES_AT_ENTITY_SET_NAME )
                .fromEntity( PERSON_ALIAS )
                .toEntity( ADDRESS_ALIAS )
                .addProperty( STRING_ID_FQN, PERSON_ID_COL )
                .endAssociation()

                .endAssociations()
                .done();

        flights.put( flight, payload );
        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com" );

        missionControl.prepare( flights, false, ImmutableSet.of() ).launch( 10000 );
        MissionControl.succeed();

    }

    private static final String getLastName( Object obj ) {
        if ( obj == null )
            return null;
        String[] names = obj.toString().split( "," );
        if ( names.length < 1 )
            return null;
        return names[ 0 ];
    }

    private static final String getSuffix( Object obj ) {
        if ( obj == null )
            return null;
        String[] names = obj.toString().split( "," );
        if ( names.length < 3 )
            return null;
        return names[ 2 ];
    }

    private static String[] getFirstAndMiddleNames( Object obj ) {
        if ( obj == null )
            return new String[] {};
        String[] names = obj.toString().split( "," );
        if ( names.length < 2 )
            return new String[] {};
        return names[ 1 ].split( " " );
    }

    private static String getFirstName( Object obj ) {
        String[] names = getFirstAndMiddleNames( obj );
        if ( names == null || names.length < 2 )
            return null;
        return names[ 1 ];
    }

    private static String getMiddleName( Object obj ) {
        String[] names = getFirstAndMiddleNames( obj );
        if ( names == null || names.length < 3 )
            return null;
        return Arrays.stream( Arrays.copyOfRange( names, 2, names.length ) ).collect( Collectors.joining( " " ) );
    }
}

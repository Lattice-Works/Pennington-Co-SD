package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.auth0.Auth0Delegate;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.integrations.pennington.utils.EdmConstants;
import com.openlattice.integrations.pennington.utils.IntegrationAliases;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.MissionParameters;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.payload.CsvPayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.util.Parsers;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class OdysseyCasesDailyDump {

    protected static final Logger logger = LoggerFactory.getLogger( OdysseyCasesDailyDump.class );

    public static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PROD_INTEGRATION;

    private static final DateTimeHelper bdHelper = new DateTimeHelper( DateTimeZone.forOffsetHours( -6 ),
            "yyyy-MM-dd HH:mm:ss" );

    private static final SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String filePath = args[ 2 ];

        final Auth0Configuration auth0Configuration = ResourceConfigurationLoader.loadConfigurationFromResource( "auth0.yaml", Auth0Configuration.class );
        final Auth0Delegate auth0Client = Auth0Delegate.fromConfig( auth0Configuration );
        String jwtToken = auth0Client.getIdToken( username, password );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Map<Flight, Payload> flights = Maps.newHashMap();
        Payload payload = new CsvPayload( filePath );

        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( IntegrationAliases.PERSON_ALIAS )
                .to( EdmConstants.PEOPLE_ENTITY_SET )
                .addProperty( EdmConstants.PERSON_ID_FQN, IntegrationAliases.PERSON_ID_COL )
                .addProperty( EdmConstants.FIRST_NAME_FQN, IntegrationAliases.FIRST_NAME_COL )
                .addProperty( EdmConstants.MIDDLE_NAME_FQN, IntegrationAliases.MIDDLE_NAME_COL )
                .addProperty( EdmConstants.LAST_NAME_FQN, IntegrationAliases.LAST_NAME_COL )
                .addProperty( EdmConstants.SUFFIX_FQN, IntegrationAliases.SUFFIX_NAME_COL )
                .addProperty( EdmConstants.DOB_FQN ).value( row -> bdHelper.parseDate( row.getAs( IntegrationAliases.DOB_COL ) ) ).ok()
                .addProperty( EdmConstants.RACE_FQN, IntegrationAliases.RACE_KEY_COL )
                .addProperty( EdmConstants.ETHNICITY_FQN, IntegrationAliases.ETHNICITY_COL )
                .addProperty( EdmConstants.EYE_FQN, IntegrationAliases.EYE_COL )
                .addProperty( EdmConstants.GENDER_FQN, IntegrationAliases.GENDER_COL )
                .addProperty( EdmConstants.HEIGHT_FQN ).value( row -> Parsers.parseInt( row.getAs( IntegrationAliases.HEIGHT_COL ) ) ).ok()
                .addProperty( EdmConstants.WEIGHT_FQN ).value( row -> Parsers.parseInt( row.getAs( IntegrationAliases.WEIGHT_COL ) ) ).ok()
                .endEntity()

                .addEntity( IntegrationAliases.CASE_ALIAS )
                .to( EdmConstants.CASES_ENTITY_SET )
                .entityIdGenerator( row -> row.get( IntegrationAliases.CASE_NO_COL ).toString() )
                .addProperty( EdmConstants.CASE_NO_FQN, IntegrationAliases.CASE_NO_COL )
                .endEntity()

                .addEntity( IntegrationAliases.ADDRESS_ALIAS )
                .to( EdmConstants.ADDRESSES_ENTITY_SET )
                .addProperty( EdmConstants.ADDRESS_FQN, IntegrationAliases.ADDRESS_COL )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                .to( EdmConstants.APPEARS_IN_ENTITY_SET )
                .fromEntity( IntegrationAliases.PERSON_ALIAS )
                .toEntity( IntegrationAliases.CASE_ALIAS )
                .addProperty( EdmConstants.STRING_ID_FQN, IntegrationAliases.CASE_NO_COL )
                .addProperty( EdmConstants.DATETIME_FQN ).value( row -> bdHelper.parse( format.format( new Date() ) ) ).ok()
                .endAssociation()

                .addAssociation( IntegrationAliases.LIVES_AT_ALIAS )
                .to( EdmConstants.LIVES_AT_ENTITY_SET )
                .fromEntity( IntegrationAliases.PERSON_ALIAS )
                .toEntity( IntegrationAliases.ADDRESS_ALIAS )
                .addProperty( EdmConstants.STRING_ID_FQN, IntegrationAliases.PERSON_ID_COL )
                .endAssociation()

                .endAssociations()
                .done();

        flights.put( flight, payload );
        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com",
                MissionParameters.empty() );
        missionControl.prepare( flights, false, ImmutableMap.of(), ImmutableSet.of()).launch( 10000 );
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

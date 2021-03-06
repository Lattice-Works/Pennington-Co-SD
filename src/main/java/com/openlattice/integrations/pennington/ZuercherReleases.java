package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.auth0.Auth0Delegate;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.UpdateType;
import com.openlattice.integrations.pennington.configurations.ReleaseIntegrationConfiguration;
import com.openlattice.integrations.pennington.utils.*;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.MissionParameters;
import com.openlattice.shuttle.Row;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.CsvPayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.util.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ZuercherReleases {
    private static final Logger logger                              = LoggerFactory.getLogger( ZuercherReleases.class );
    private static final RetrofitFactory.Environment environment    = RetrofitFactory.Environment.PROD_INTEGRATION;

    public static County county;
    private static final String[] dateTimePatterns         = new String[] { "MM/dd/yy HH:mm" };
    private static final JavaDateTimeHelper bdHelper    = new JavaDateTimeHelper(
            TimeZones.America_Denver,
            dateTimePatterns,
            true
        );


    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String inmatesPath = args[ 2 ];

        final Auth0Configuration auth0Configuration = ResourceConfigurationLoader.loadConfigurationFromResource( "auth0.yaml", Auth0Configuration.class );
        final Auth0Delegate auth0Client = Auth0Delegate.fromConfig( auth0Configuration );
        final String jwtToken = auth0Client.getIdToken( username, password );

        CsvPayload payload = new CsvPayload( inmatesPath );

        county = County.valueOf( args[ 3 ] );

        final ReleaseIntegrationConfiguration config = ReleaseIntegrationConfigurations.CONFIGURATIONS.get( county );

//        String jwtToken = "";

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Flight releaseFlight = Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.ARRESTEE_ALIAS)
                .to( config.getPeople() )
                .updateType( UpdateType.Merge )
                .addProperty( EdmConstants.PERSON_ID_FQN, ZuercherConstants.PARTY_ID )
                .addProperty( EdmConstants.JACKET_NO_FQN, ZuercherConstants.PERSON_JACKET_ID )
                .addProperty( EdmConstants.LAST_NAME_FQN, ZuercherConstants.LAST_NAME )
                .addProperty( EdmConstants.FIRST_NAME_FQN, ZuercherConstants.FIRST_NAME )
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
                .addEntity( IntegrationAliases.JAIL_STAY_ALIAS )
                .to( config.getJailStays() )
                .updateType( UpdateType.PartialReplace )
                .entityIdGenerator( IntegrationUtils::getInmateID )
                .addProperty( EdmConstants.OL_ID_FQN, ZuercherConstants.INMATE_NUMBER )
                .addProperty( EdmConstants.RELEASE_DATE_TIME_FQN )
                .value( ZuercherReleases::getReleaseDateTime ).ok()
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( IntegrationAliases.SUBJECT_OF_ALIAS )
                .to( config.getSubjectOf() )
                .updateType( UpdateType.Replace )
                .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                .toEntity( IntegrationAliases.JAIL_STAY_ALIAS )
                .addProperty( EdmConstants.OL_ID_FQN ).value( IntegrationUtils::getInmateID ).ok()
                .endAssociation()
                .endAssociations()
                .done();
        //@formatter:on

        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com",
                MissionParameters.empty() );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( releaseFlight, payload );
        missionControl.prepare( flights, false, ImmutableMap.of(), ImmutableSet.of() ).launch( 10000 );
        MissionControl.succeed();
    }

    private static String getReleaseDateTime( Row row ) {
        JavaDateTimeHelper DTHelper = IntegrationUtils.getDtHelperForCounty( county, dateTimePatterns );

        String datTimeStr = Parsers.getAsString( row.getAs( ZuercherConstants.RELEASE_DATE ) );
        if ( county == null || datTimeStr == null ) {
            logger.debug( "Unable to get datetime." );
            return null;
        }

        String dateTimeStr = datTimeStr.trim();
        return Parsers.getAsString( DTHelper.parseDateTime( dateTimeStr ) );
    }
}

package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.UpdateType;
import com.openlattice.integrations.pennington.configurations.InmateIntegrationConfiguration;
import com.openlattice.integrations.pennington.utils.*;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.adapter.Row;
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

public class ZuercherInmates {
    private static final Logger logger      = LoggerFactory.getLogger( ZuercherInmates.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PROD_INTEGRATION;

    public static County county;
    private static final String dateTimePattern         = "MM/dd/yy HH:mm";
    private static final JavaDateTimeHelper bdHelper    = new JavaDateTimeHelper( TimeZones.America_Denver,"MM/dd/yy" );


    public static void integrate( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 0 ];
        final String password = args[ 1 ];
        final String inmatesPath = args[ 2 ];
        String jwtToken = MissionControl.getIdToken( username, password );

        SimplePayload payload = new SimplePayload( inmatesPath );

        county = County.valueOf( args[ 3 ] );
        final InmateIntegrationConfiguration config = InmateIntegrationConfigurations.CONFIGURATIONS.get( county );


        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Flight inmatesFlight = Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.ARRESTEE_ALIAS)
                .to( config.getPeople() )
                .updateType( UpdateType.Merge )
                .addProperty( EdmConstants.PERSON_ID_FQN, ZuercherConstants.PARTY_ID )
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
                .updateType( UpdateType.Replace )
                .entityIdGenerator( IntegrationUtils::getInmateID )
                .addProperty( EdmConstants.OL_ID_FQN, ZuercherConstants.INMATE_NUMBER )
                .addProperty( EdmConstants.BOOKING_DATE_FQN )
                .value( ZuercherInmates::getBookingDateTime ).ok()
                .addProperty( EdmConstants.RELEASE_DATE_TIME_FQN )
                .value( ZuercherInmates::getSentenceEndDateTime ).ok()
                .endEntity()
                .addEntity( IntegrationAliases.BOND_ALIAS )
                .to( config.getBonds() )
                .updateType( UpdateType.Replace )
                .entityIdGenerator( ZuercherInmates::getBondId )
                .addProperty( EdmConstants.BOND_AMOUNT_FQN ).value( ZuercherInmates::getBondAmount ).ok()
                .addProperty( EdmConstants.SURETY_AMOUNT_FQN ).value( ZuercherInmates::getSuretyAmount ).ok()
                .addProperty( EdmConstants.BOND_DESCRIPTION_FQN).value( row -> row.getAs( ZuercherConstants.BOND_TYPE ) ).ok()
                .addProperty( EdmConstants.BOND_SOURCE).value( row -> row.getAs( ZuercherConstants.BOND_SOURCE ) ).ok()
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( IntegrationAliases.SUBJECT_OF_ALIAS )
                .to( config.getSubjectOf() )
                .updateType( UpdateType.Replace )
                .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                .toEntity( IntegrationAliases.JAIL_STAY_ALIAS )
                .addProperty( EdmConstants.COMPLETED_DATETIME_FQN ).value( ZuercherInmates::getBookingDateTime ).ok()
                .addProperty( EdmConstants.OL_ID_FQN ).value( IntegrationUtils::getInmateID ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.REGISTERED_FOR_ALIAS )
                .to( config.getRegisteredfor() )
                .updateType( UpdateType.Replace )
                .fromEntity( IntegrationAliases.ARRESTEE_ALIAS )
                .toEntity( IntegrationAliases.BOND_ALIAS )
                .entityIdGenerator( ZuercherInmates::getBondId )
                .addProperty( EdmConstants.COMPLETED_DATETIME_FQN ).value( ZuercherInmates::getBookingDateTime ).ok()
                .addProperty( EdmConstants.OL_ID_FQN ).value( IntegrationUtils::getInmateID ).ok()
                .endAssociation()
                .addAssociation( IntegrationAliases.BOND_SET_ALIAS )
                .to( config.getBondSet() )
                .updateType( UpdateType.Replace )
                .fromEntity( IntegrationAliases.JAIL_STAY_ALIAS )
                .toEntity( IntegrationAliases.BOND_ALIAS )
                .addProperty( EdmConstants.OL_ID_FQN )
                .value( ZuercherInmates::getBondId ).ok()
                .endAssociation()
                .endAssociations()
                .done();
        //@formatter:on

        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com" );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( inmatesFlight, payload );
        missionControl.prepare( flights, false, ImmutableList.of(), ImmutableSet.of() ).launch( 10000 );
        MissionControl.succeed();
    }

    public static String getBondId( Map<String, Object> row ) {
        String baseId = IntegrationUtils.getInmateID( row );
        String bondDescription = Parsers.getAsString( row.getOrDefault( ZuercherConstants.BOND_TYPE, "NoBondType" ) );
        String bondNumber = Parsers.getAsString( row.get( ZuercherConstants.BOND_NUM ) );
        String finalId = baseId + "|" + bondDescription + "|" + bondNumber;
        if ( StringUtils.isNotBlank( finalId ) && StringUtils.isNotBlank( finalId.trim() ) ) {
            return finalId;
        }
        return null;
    }

    public static String getBondId( Row row ) {
        String baseId = IntegrationUtils.getInmateID( row );
        String bondDescription = Parsers.getAsString( row.getAs( ZuercherConstants.BOND_TYPE ) );
        if ( StringUtils.isBlank( bondDescription ) || bondDescription == null ) bondDescription = "NoBondType";
        String bondNumber = Parsers.getAsString( row.getAs( ZuercherConstants.BOND_NUM) );
        String finalId = baseId + "|" + bondDescription + "|" + bondNumber;
        if ( StringUtils.isNotBlank( finalId ) && StringUtils.isNotBlank( finalId.trim() ) ) {
            return finalId;
        }
        return null;
    }

    private static String getBondAmount( Row row ) {
        String bondType = Parsers.getAsString( row.getAs( ZuercherConstants.BOND_TYPE ) );
        String bondAmount = Parsers.getAsString( row.getAs( ZuercherConstants.BOND_AMOUNT ) );
        if (StringUtils.isNoneBlank( bondAmount ) && bondType.equals( "Cash Only" )) {
            return bondAmount.replaceAll("[^\\d.]+", "").trim();
        }

        return null;
    }

    private static String getSuretyAmount( Row row ) {
        String bondType = Parsers.getAsString( row.getAs( ZuercherConstants.BOND_TYPE ) );
        String bondAmount = Parsers.getAsString( row.getAs( ZuercherConstants.BOND_AMOUNT ) );
        if (StringUtils.isNoneBlank( bondAmount ) && bondType.equals( "Cash/Surety" )) {
            return bondAmount.replaceAll("[^\\d.]+", "").trim();
        }

        return null;
    }

    private static String getBookingDateTime( Row row ) {
        JavaDateTimeHelper DTHelper = IntegrationUtils.getDtHelperForCounty( county, dateTimePattern );

        String datTimeStr = Parsers.getAsString( row.getAs( ZuercherConstants.BOOKING_DATE_TIME ) );
        if ( county == null || datTimeStr == null ) {
            logger.debug( "Unable to get datetime." );
            return null;
        }

        String dateTimeStr = datTimeStr.trim();
        return Parsers.getAsString( DTHelper.parseDateTime( dateTimeStr ) );
    }

    private static Object getSentenceEndDateTime( Row row ) {
        JavaDateTimeHelper DTHelper = IntegrationUtils.getDtHelperForCounty( county, dateTimePattern );

        String datTimeStr = Parsers.getAsString( row.getAs( ZuercherConstants.SENTENCE_END_DATE_TIME ) );
        if ( county == null || datTimeStr == null ) {
            logger.debug( "Unable to get datetime." );
            return null;
        }

        String dateTimeStr = datTimeStr.trim();
        return Parsers.getAsString( DTHelper.parseDateTime( dateTimeStr ) );
    }

}

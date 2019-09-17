/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.integrations.pennington;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.client.RetrofitFactory.Environment;
import com.openlattice.integrations.pennington.utils.ChargeConstants;
import com.openlattice.integrations.pennington.utils.EdmConstants;
import com.openlattice.integrations.pennington.utils.IntegrationAliases;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.Edm;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReferenceCharges {
    protected static final Logger logger = LoggerFactory.getLogger( ReferenceCharges.class );

    public static final RetrofitFactory.Environment environment = Environment.PROD_INTEGRATION;

    private static final String STATUTE_FQN     = EdmConstants.OL_ID_FQN;
    private static final String DESCRIPTION_FQN = "ol.name";

    private static final String DEGREE_FQN       = "ol.level";
    private static final String DEGREE_SHORT_FQN = "ol.levelstate";

    private static final String VIOLENT_FQN    = "ol.violent";
    private static final String DMF_STEP_2_FQN = "ol.dmfstep2indicator";
    private static final String DMF_STEP_4_FQN = "ol.dmfstep4indicator";
    private static final String BHE_FQN        = "ol.bheindicator";
    private static final String BRE_FQN        = "ol.breindicator";

    private static final String MINN_COURT_CHARGES    = "minnehahacourtcharges";
    private static final String MINN_ARREST_CHARGES   = "minnehahaarrestcharges";
    private static final String PENN_COURT_CHARGES    = "penningtoncourtcharges";
    private static final String PENN_ARREST_CHARGES   = "penningtonarrestcharges";
    private static final String SHELBY_COURT_CHARGES  = "Shelby County_publicsafety_courtchargelist";
    private static final String SHELBY_ARREST_CHARGES = "Shelby County_publicsafety_arrestchargelist";

    private static final String LINCOLN_COURT_CHARGES  = "lincolncountysd_publicsafety_courtchargelist";
    private static final String LINCOLN_ARREST_CHARGES = "lincolncountysd_publicsafety_arrestchargelist";

    private static final String PTCM_ARREST_CHARGES = "OpenLattice PCM Demo Org_publicsafety_arrestchargelist";
    private static final String PTCM_COURT_CHARGES  = "OpenLattice PCM Demo Org_publicsafety_courtchargelist";

    private static final String SD_APPEARS_IN      = "southdakotaappearsin";
    private static final String COUNTIES           = "southdakotacounties";
    private static final String COURTHOUSES        = "southdakotacourthouses";
    private static final String COURTROOMS         = "southdakotacourtrooms";
    private static final String AGENCIES           = "southdakotaagencies";
    private static final String REMINDER_TEMPLATES = "southdakotaremindertemplates";
    private static final String CONTACT_INFO       = "southdakotacontactinformation";
    private static final String CONTACT_INFO_GIVEN = "southdakotacontactinfogiven";

    private static String getChargeId( Map<String, Object> row ) {
        String statute = Parsers.getAsString( row.get( ChargeConstants.STATUTE ) );
        String description = Parsers.getAsString( row.get( ChargeConstants.DESCRIPTION ) );
        if ( statute == null ) {
            statute = "";
        }
        if ( description == null ) {
            description = "";
        }

        return statute + "|" + description;
    }

    private static boolean getIsViolent( Row row ) {
        return Parsers.parseBoolean( row.getAs(ChargeConstants.IS_VIOLENT) );
    }


    public static String getValue( Row row, String header ) {
        return Parsers.getAsString( row.getAs( header ) );
    }

    public static Flight getSDCounties() {
        return Flight.newFlight()
                .createEntities()
            .addEntity( IntegrationAliases.COUNTY_ALIAS )
                .to( COUNTIES )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.ID )
                .addProperty( EdmConstants.NAME_FQN, IntegrationAliases.COUNTY_ALIAS )
                .endEntity()
                .endEntities()
                .done();
    }

    public static Flight getSDCourthouses() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.COUNTY_ALIAS )
                .to( COUNTIES )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.ID )
                .endEntity()
                .addEntity( IntegrationAliases.COURTHOUSE_ALIAS )
                .to( COURTHOUSES )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.ID )
                .addProperty( EdmConstants.NAME_FQN, ChargeConstants.NAME)
                .addProperty( EdmConstants.ADDRESS_FQN, ChargeConstants.ADDRESS )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                .to( SD_APPEARS_IN )
                .fromEntity( IntegrationAliases.COURTHOUSE_ALIAS )
                .toEntity( IntegrationAliases.COUNTY_ALIAS )
                .addProperty( EdmConstants.STRING_ID_FQN, ChargeConstants.ID )
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static Flight getSDCourtPhones() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.COURTHOUSE_ALIAS )
                .to( COURTHOUSES )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.ID )
                .endEntity()
                .addEntity( IntegrationAliases.PHONE_ALIAS )
                .to( CONTACT_INFO )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.PHONE )
                .addProperty( EdmConstants.PHONE_FQN, ChargeConstants.PHONE )
                .addProperty( EdmConstants.PREFERRED_FQN, ChargeConstants.PREFERRED )
                .addProperty( EdmConstants.CELL_PHONE_FQN ).value( row -> false ).ok()
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( IntegrationAliases.CONTACT_INFO_GIVEN_ALIAS )
                .to( CONTACT_INFO_GIVEN )
                .fromEntity( IntegrationAliases.PHONE_ALIAS )
                .toEntity( IntegrationAliases.COURTHOUSE_ALIAS )
                .addProperty( EdmConstants.OL_ID_FQN, ChargeConstants.PHONE )
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static Flight getSDCourtrooms() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.COURTROOM_ALIAS )
                .to( COURTROOMS )
                .addProperty( EdmConstants.OL_ID_FQN ).value( row -> row.getAs( ChargeConstants.ID ) + "|" + row.getAs( ChargeConstants.ROOM ) ).ok()
                .addProperty( EdmConstants.ROOM_NO_FQN, ChargeConstants.ROOM )
                .endEntity()
                .addEntity( IntegrationAliases.COURTHOUSE_ALIAS )
                .to( COURTHOUSES )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.ID )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                .to( SD_APPEARS_IN )
                .fromEntity( IntegrationAliases.COURTROOM_ALIAS )
                .toEntity( IntegrationAliases.COURTHOUSE_ALIAS )
                .addProperty( EdmConstants.STRING_ID_FQN ).value( row -> row.getAs( ChargeConstants.ID ) + "|" + row.getAs( ChargeConstants.ROOM ) ).ok()
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static Flight getSDAgencies() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.COUNTY_ALIAS )
                .to( COUNTIES )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.ID )
                .endEntity()
                .addEntity( IntegrationAliases.AGENCY_ALIAS )
                .to( AGENCIES )
                .addProperty( EdmConstants.OL_ID_FQN, ChargeConstants.ABBREV )
                .addProperty( EdmConstants.NAME_FQN, ChargeConstants.NAME )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                .to( SD_APPEARS_IN )
                .fromEntity( IntegrationAliases.AGENCY_ALIAS  )
                .toEntity( IntegrationAliases.COUNTY_ALIAS )
                .addProperty( EdmConstants.STRING_ID_FQN, ChargeConstants.ID )
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static Flight getCourtChargeFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "minnehahaCourtCharge" )
                .to( MINN_COURT_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, ChargeConstants.STATUTE ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, ChargeConstants.DESCRIPTION ) ).ok()
                .addProperty( VIOLENT_FQN ).value( ReferenceCharges::getIsViolent ).ok()
                .endEntity()

                .addEntity( "penningtonCourtCharge" )
                .to( PENN_COURT_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, ChargeConstants.STATUTE ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, ChargeConstants.DESCRIPTION ) ).ok()
                .addProperty( VIOLENT_FQN ).value( ReferenceCharges::getIsViolent ).ok()
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getArrestChargeFlight( String entitySetName ) {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "arrestCharge" )
                .to( entitySetName )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN, ChargeConstants.STATUTE )
                .addProperty( DESCRIPTION_FQN, ChargeConstants.DESCRIPTION )
                .addProperty( DEGREE_FQN, ChargeConstants.DEGREE )
                .addProperty( DEGREE_SHORT_FQN, ChargeConstants.DEGREE_SHORT )
                .addProperty( DMF_STEP_2_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.STEP_2 ) ) ).ok()
                .addProperty( DMF_STEP_4_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.STEP_4 ) ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.IS_VIOLENT ) ) ).ok()
                .addProperty( BHE_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.BHE ) ) ).ok()
                .addProperty( BRE_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.BRE ) ) ).ok()
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getPennArrestChargeFlight20190207() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "arrestCharge" )
                .to( PENN_ARREST_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN, ChargeConstants.STATUTE )
                .addProperty( DESCRIPTION_FQN, ChargeConstants.DESCRIPTION )
                .addProperty( DEGREE_FQN, ChargeConstants.DEGREE )
                .addProperty( DEGREE_SHORT_FQN, ChargeConstants.DEGREE_SHORT )
                .addProperty( DMF_STEP_2_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.STEP_2 ) ) ).ok()
                .addProperty( DMF_STEP_4_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.STEP_4  ) ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.IS_VIOLENT ) ) ).ok()
                .addProperty( BHE_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.BHE ) ) ).ok()
                .addProperty( BRE_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.BRE ) ) ).ok()
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getPTCMDemoArrestChargeFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "arrestCharge" )
                .to( PTCM_ARREST_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN, ChargeConstants.STATUTE )
                .addProperty( DESCRIPTION_FQN, ChargeConstants.DESCRIPTION )
                .addProperty( DEGREE_FQN, ChargeConstants.DEGREE )
                .addProperty( DEGREE_SHORT_FQN, ChargeConstants.DEGREE_SHORT )
                .addProperty( DMF_STEP_2_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.STEP_2 ) ) ).ok()
                .addProperty( DMF_STEP_4_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.STEP_4 ) ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.IS_VIOLENT ) ) ).ok()
                .addProperty( BHE_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.BHE ) ) ).ok()
                .addProperty( BRE_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.BRE ) ) ).ok()
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getPTCMDemoCourtChargeFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "courtCharge" )
                .to( PTCM_COURT_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, ChargeConstants.STATUTE ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, ChargeConstants.DESCRIPTION ) ).ok()
                .addProperty( VIOLENT_FQN ).value( ReferenceCharges::getIsViolent ).ok()
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getLincolnCourtChargeFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "courtCharge" )
                .to( LINCOLN_COURT_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, ChargeConstants.STATUTE ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, ChargeConstants.DESCRIPTION ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> Parsers.parseBoolean( row.getAs( ChargeConstants.IS_VIOLENT ) ) ).ok()
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getDMFAndViolentFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "minnCharge" )
                .to( MINN_ARREST_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN, ChargeConstants.STATUTE )
                .addProperty( DESCRIPTION_FQN, ChargeConstants.DESCRIPTION )
                .addProperty( VIOLENT_FQN, ChargeConstants.IS_VIOLENT )
                .addProperty( DMF_STEP_2_FQN, ChargeConstants.STEP_2 )
                .addProperty( DMF_STEP_4_FQN, ChargeConstants.STEP_4 )
                .endEntity()

                .addEntity( "pennCharge" )
                .to( PENN_ARREST_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN, ChargeConstants.STATUTE )
                .addProperty( DESCRIPTION_FQN, ChargeConstants.DESCRIPTION )
                .addProperty( VIOLENT_FQN, ChargeConstants.IS_VIOLENT )
                .addProperty( DMF_STEP_2_FQN, ChargeConstants.STEP_2 )
                .addProperty( DMF_STEP_4_FQN, ChargeConstants.STEP_4 )
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getBookingExceptionFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "bookingCharge" )
                .to( PENN_ARREST_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN, ChargeConstants.STATUTE )
                .addProperty( DESCRIPTION_FQN, ChargeConstants.DESCRIPTION )
                .addProperty( BHE_FQN, ChargeConstants.BHE )
                .addProperty( BRE_FQN, ChargeConstants.BRE )
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getFakeFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "bookingCharge" )
                .to( "testclear" )
                .addProperty( EdmConstants.PERSON_ID_FQN, "a" )
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getShelbyCourtChargesFlight() {

        return Flight.newFlight()
                .createEntities()

                .addEntity( IntegrationAliases.CHARGE_ALIAS )
                .to( SHELBY_COURT_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN ).value( row -> Parsers.getAsString( row.getAs( ChargeConstants.STATUTE ) ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> Parsers.getAsString( row.getAs( ChargeConstants.DESCRIPTION ) ) ).ok()
                .addProperty( VIOLENT_FQN )
                .value( row -> row.getAs( ChargeConstants.IS_VIOLENT ).toString().toLowerCase().contains( "yes" ) ).ok()
                .addProperty( DEGREE_FQN, ChargeConstants.DESCRIPTION )
                .addProperty( DEGREE_SHORT_FQN, ChargeConstants.DEGREE )
                .endEntity()

                .endEntities()
                .done();

    }

    public static Flight getJudgesFlight() {

        return Flight.newFlight()
                .createEntities()
                .addEntity( IntegrationAliases.JUDGE_ALIAS )
                .to( EdmConstants.JUDGE_ENTITY_SET )
                .addProperty( EdmConstants.PERSON_ID_FQN, ChargeConstants.ID )
                .addProperty( EdmConstants.FIRST_NAME_FQN, ChargeConstants.FIRST_NAME )
                .addProperty( EdmConstants.LAST_NAME_FQN, ChargeConstants.LAST_NAME )
                .addProperty( EdmConstants.MIDDLE_NAME_FQN, ChargeConstants.MIDDLE_NAME )
                .addProperty( EdmConstants.JURISDICTION_FQN, ChargeConstants.COUNTY )
                .endEntity()
                .endEntities()
                .done();
    }

    public static Flight getReminderTemplatesFlight() {

        return Flight.newFlight()
                .createEntities()
                .addEntity( "remindertemplate" )
                .to( REMINDER_TEMPLATES )
                .addProperty( EdmConstants.OL_ID_FQN, ChargeConstants.ID )
                .addProperty( EdmConstants.TEXT_FQN, ChargeConstants.TEXT )
                .addProperty( EdmConstants.TYPE_FQN, ChargeConstants.TYPE )
                .addProperty( EdmConstants.TIME_IN_ADVANCE_FQN, ChargeConstants.DURATION )
                .endEntity()
                .addEntity( IntegrationAliases.COUNTY_ALIAS )
                .to( COUNTIES )
                .addProperty( EdmConstants.GENERAL_ID_FQN, ChargeConstants.COUNTY_ID )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( IntegrationAliases.APPEARS_IN_ALIAS )
                .to( SD_APPEARS_IN )
                .fromEntity( "remindertemplate" )
                .toEntity( IntegrationAliases.COUNTY_ALIAS )
                .addProperty( EdmConstants.STRING_ID_FQN, ChargeConstants.ID )
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static void main( String[] args ) throws InterruptedException {


        final String jwtToken = "";

        final String lincolnArrestChargeFilePath = "";
        final String lincolnCourtChargeFilePath = "";
        final String courtChargeFilePath = "";
        final String minnehahaArrestChargeFilePath = "";
        final String penningtonArrestChargeFilePath = "";
        final String violentStep2Step4ChargeFilePath = "";
        final String bookingExceptionChargeFilePath = "";
        final String shelbyCourtChargesFilePath = "";
        final String penningtonArrestCourtCharges20190207FilePath = "";
        final String judgesFilePath = "";
        final String sdCountiesPath = "";
        final String sdCourthousesPath = "";
        final String sdAgenciesPath = "";
        final String sdCourtroomsPath = "";
        final String sdReminderTemplates = "";
        final String sdCourtPhones = "";

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Map<Flight, Payload> flights = Maps.newHashMap();


        /* for court charges */
//                flights.put( getCourtChargeFlight(), new SimplePayload( courtChargeFilePath ) );
        /* for lincoln court charges */
//                flights.put( getLincolnCourtChargeFlight(), new SimplePayload( lincolnCourtChargeFilePath ) );

        /* for minnehaha arrest charges*/
                flights.put( getArrestChargeFlight( LINCOLN_ARREST_CHARGES ), new SimplePayload( lincolnArrestChargeFilePath ) );

        //        /* for pennington arrest charges */
        //        //        flights.put( getArrestChargeFlight( PENN_ARREST_CHARGES ), new SimplePayload( penningtonArrestChargeFilePath ) );
        //        flights.put( getPennArrestChargeFlight20190207(),
        //                new SimplePayload( penningtonArrestCourtCharges20190207FilePath ) );

        /* for arrest violent/step2/step4 values */
        //        flights.put( getDMFAndViolentFlight(), new SimplePayload( violentStep2Step4ChargeFilePath ) );

        /* for BHE/BRE values */
        //        flights.put( getBookingExceptionFlight(), new SimplePayload( bookingExceptionChargeFilePath ) );

        /* for Shelby court charges */
        //        flights.put( getShelbyCourtChargesFlight(), new SimplePayload( shelbyCourtChargesFilePath ) );


        /* PTCM Demo Org Charges */
        //        flights.put( getPTCMDemoArrestChargeFlight(),
        //                new SimplePayload( penningtonArrestCourtCharges20190207FilePath ) );
        //        flights.put( getPTCMDemoCourtChargeFlight(), new SimplePayload( courtChargeFilePath ) );

        //        flights.put( getSDCounties(), new SimplePayload( sdCountiesPath ) );
        //        flights.put( getSDCourthouses(), new SimplePayload( sdCourthousesPath ) );
        //        flights.put( getSDAgencies(), new SimplePayload( sdAgenciesPath ) );
        //        flights.put( getSDCourtrooms(), new SimplePayload( sdCourtroomsPath ) );
        //                flights.put( getReminderTemplatesFlight(), new SimplePayload( sdReminderTemplates ) );
        //                flights.put( getSDCourtPhones(), new SimplePayload( sdCourtPhones ) );


        /* for judges */
        //        flights.put( getJudgesFlight(), new SimplePayload( judgesFilePath ) );

        MissionControl missionControl = new MissionControl( environment,
                () -> jwtToken,
                "https://openlattice-media-storage.s3.us-gov-west-1.amazonaws.com" );
        missionControl.prepare( flights, false, List.of(), Set.of()).launch( 150 );
        MissionControl.succeed();

    }

}
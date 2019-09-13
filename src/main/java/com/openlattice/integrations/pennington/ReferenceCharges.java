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
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
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

    private static final String STATUTE_FQN     = "ol.id";
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
        String statute = Parsers.getAsString( row.get( "statute" ) );
        String description = Parsers.getAsString( row.get( "description" ) );
        if ( statute == null ) {
            statute = "";
        }
        if ( description == null ) {
            description = "";
        }

        return statute + "|" + description;
    }

    private static boolean getIsViolent( Row row ) {
        return row.getAs( "violent" ).equals( "True" );
    }

    private static boolean getBooleanValue( Object value ) {
        String strValue = Parsers.getAsString( value );
        if ( StringUtils.isNotBlank( strValue ) ) {
            return strValue.trim().toLowerCase().equals( "true" );
        }

        return false;
    }

    public static String getValue( Row row, String header ) {
        String val = row.getAs( header );
        if ( val == null )
            return null;
        return val.trim();
    }

    public static Flight getSDCounties() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( "county" )
                .to( COUNTIES )
                .addProperty( "general.id", "id" )
                .addProperty( "ol.name", "county" )
                .endEntity()
                .endEntities()
                .done();
    }

    public static Flight getSDCourthouses() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( "county" )
                .to( COUNTIES )
                .addProperty( "general.id", "id" )
                .endEntity()
                .addEntity( "courthouse" )
                .to( COURTHOUSES )
                .addProperty( "general.id", "id" )
                .addProperty( "ol.name", "name" )
                .addProperty( "location.address", "address" )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( "appearsin" )
                .to( SD_APPEARS_IN )
                .fromEntity( "courthouse" )
                .toEntity( "county" )
                .addProperty( "general.stringid", "id" )
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static Flight getSDCourtPhones() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( "courthouse" )
                .to( COURTHOUSES )
                .addProperty( "general.id", "id" )
                .endEntity()
                .addEntity( "phone" )
                .to( CONTACT_INFO )
                .addProperty( "general.id", "phone" )
                .addProperty( "contact.phonenumber", "phone" )
                .addProperty( "ol.preferred", "preferred" )
                .addProperty( "contact.cellphone" ).value( row -> false ).ok()
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( "contactinfogiven" )
                .to( CONTACT_INFO_GIVEN )
                .fromEntity( "phone" )
                .toEntity( "courthouse" )
                .addProperty( "ol.id", "phone" )
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static Flight getSDCourtrooms() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( "courtroom" )
                .to( COURTROOMS )
                .addProperty( "ol.id" ).value( row -> row.getAs( "id" ) + "|" + row.getAs( "room" ) ).ok()
                .addProperty( "ol.roomnumber", "room" )
                .endEntity()
                .addEntity( "courthouse" )
                .to( COURTHOUSES )
                .addProperty( "general.id", "id" )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( "appearsin" )
                .to( SD_APPEARS_IN )
                .fromEntity( "courtroom" )
                .toEntity( "courthouse" )
                .addProperty( "general.stringid" ).value( row -> row.getAs( "id" ) + "|" + row.getAs( "room" ) ).ok()
                .endAssociation()
                .endAssociations()
                .done();
    }

    public static Flight getSDAgencies() {
        return Flight.newFlight()
                .createEntities()
                .addEntity( "county" )
                .to( COUNTIES )
                .addProperty( "general.id", "id" )
                .endEntity()
                .addEntity( "agency" )
                .to( AGENCIES )
                .addProperty( "ol.id", "abbrev" )
                .addProperty( "ol.name", "name" )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( "appearsin" )
                .to( SD_APPEARS_IN )
                .fromEntity( "agency" )
                .toEntity( "county" )
                .addProperty( "general.stringid", "id" )
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
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, "statute" ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, "description" ) ).ok()
                .addProperty( VIOLENT_FQN ).value( ReferenceCharges::getIsViolent ).ok()
                .endEntity()

                .addEntity( "penningtonCourtCharge" )
                .to( PENN_COURT_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, "statute" ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, "description" ) ).ok()
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
                .addProperty( STATUTE_FQN, "statute" )
                .addProperty( DESCRIPTION_FQN, "description" )
                .addProperty( DEGREE_FQN, "degree" )
                .addProperty( DEGREE_SHORT_FQN, "degreeShort" )
                .addProperty( DMF_STEP_2_FQN ).value( row -> getBooleanValue( row.getAs( "STEP_TWO" ) ) ).ok()
                .addProperty( DMF_STEP_4_FQN ).value( row -> getBooleanValue( row.getAs( "STEP_FOUR" ) ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> getBooleanValue( row.getAs( "ALL_VIOLENT" ) ) ).ok()
                .addProperty( BHE_FQN ).value( row -> getBooleanValue( row.getAs( "BHE Charges" ) ) ).ok()
                .addProperty( BRE_FQN ).value( row -> getBooleanValue( row.getAs( "BRE Charges" ) ) ).ok()
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
                .addProperty( STATUTE_FQN, "statute" )
                .addProperty( DESCRIPTION_FQN, "description" )
                .addProperty( DEGREE_FQN, "degree" )
                .addProperty( DEGREE_SHORT_FQN, "degreeShort" )
                .addProperty( DMF_STEP_2_FQN ).value( row -> getBooleanValue( row.getAs( "STEP_TWO" ) ) ).ok()
                .addProperty( DMF_STEP_4_FQN ).value( row -> getBooleanValue( row.getAs( "STEP_FOUR" ) ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> getBooleanValue( row.getAs( "ALL_VIOLENT" ) ) ).ok()
                .addProperty( BHE_FQN ).value( row -> getBooleanValue( row.getAs( "BHE Charges" ) ) ).ok()
                .addProperty( BRE_FQN ).value( row -> getBooleanValue( row.getAs( "BRE Charges" ) ) ).ok()
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
                .addProperty( STATUTE_FQN, "statute" )
                .addProperty( DESCRIPTION_FQN, "description" )
                .addProperty( DEGREE_FQN, "degree" )
                .addProperty( DEGREE_SHORT_FQN, "degreeShort" )
                .addProperty( DMF_STEP_2_FQN ).value( row -> getBooleanValue( row.getAs( "STEP_TWO" ) ) ).ok()
                .addProperty( DMF_STEP_4_FQN ).value( row -> getBooleanValue( row.getAs( "STEP_FOUR" ) ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> getBooleanValue( row.getAs( "ALL_VIOLENT" ) ) ).ok()
                .addProperty( BHE_FQN ).value( row -> getBooleanValue( row.getAs( "BHE Charges" ) ) ).ok()
                .addProperty( BRE_FQN ).value( row -> getBooleanValue( row.getAs( "BRE Charges" ) ) ).ok()
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
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, "statute" ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, "description" ) ).ok()
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
                .addProperty( STATUTE_FQN ).value( row -> getValue( row, "statute" ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> getValue( row, "description" ) ).ok()
                .addProperty( VIOLENT_FQN ).value( row -> getBooleanValue( row.getAs( "ALL_VIOLENT" ) ) ).ok()
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
                .addProperty( STATUTE_FQN, "statute" )
                .addProperty( DESCRIPTION_FQN, "description" )
                .addProperty( VIOLENT_FQN, "violent" )
                .addProperty( DMF_STEP_2_FQN, "step2" )
                .addProperty( DMF_STEP_4_FQN, "step4" )
                .endEntity()

                .addEntity( "pennCharge" )
                .to( PENN_ARREST_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN, "statute" )
                .addProperty( DESCRIPTION_FQN, "description" )
                .addProperty( VIOLENT_FQN, "violent" )
                .addProperty( DMF_STEP_2_FQN, "step2" )
                .addProperty( DMF_STEP_4_FQN, "step4" )
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
                .addProperty( STATUTE_FQN, "statute" )
                .addProperty( DESCRIPTION_FQN, "description" )
                .addProperty( BHE_FQN, "bhe" )
                .addProperty( BRE_FQN, "bre" )
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getFakeFlight() {
        return Flight.newFlight()
                .createEntities()

                .addEntity( "bookingCharge" )
                .to( "testclear" )
                .addProperty( "nc.SubjectIdentification", "a" )
                .endEntity()

                .endEntities()
                .done();
    }

    public static Flight getShelbyCourtChargesFlight() {

        return Flight.newFlight()
                .createEntities()

                .addEntity( "charge" )
                .to( SHELBY_COURT_CHARGES )
                .entityIdGenerator( ReferenceCharges::getChargeId )
                .addProperty( STATUTE_FQN ).value( row -> Parsers.getAsString( row.getAs( "statute" ) ) ).ok()
                .addProperty( DESCRIPTION_FQN ).value( row -> Parsers.getAsString( row.getAs( "description" ) ) ).ok()
                .addProperty( VIOLENT_FQN )
                .value( row -> row.getAs( "isViolent" ).toString().toLowerCase().contains( "yes" ) ).ok()
                .addProperty( DEGREE_FQN, "Description" )
                .addProperty( DEGREE_SHORT_FQN, "Degree" )
                .endEntity()

                .endEntities()
                .done();

    }

    public static Flight getJudgesFlight() {

        return Flight.newFlight()
                .createEntities()
                .addEntity( "judge" )
                .to( "southdakotajudges" )
                .addProperty( "nc.SubjectIdentification", "id" )
                .addProperty( "nc.PersonGivenName", "firstName" )
                .addProperty( "nc.PersonSurName", "lastName" )
                .addProperty( "nc.PersonMiddleName", "middleName" )
                .addProperty( "ol.idjurisdiction", "county" )
                .endEntity()
                .endEntities()
                .done();
    }

    public static Flight getReminderTemplatesFlight() {

        return Flight.newFlight()
                .createEntities()
                .addEntity( "remindertemplate" )
                .to( REMINDER_TEMPLATES )
                .addProperty( "ol.id", "id" )
                .addProperty( "ol.text", "text" )
                .addProperty( "ol.type", "type" )
                .addProperty( "ol.timeinadvance", "duration" )
                .endEntity()
                .addEntity( "county" )
                .to( COUNTIES )
                .addProperty( "general.id", "countyid" )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( "appearsin" )
                .to( SD_APPEARS_IN )
                .fromEntity( "remindertemplate" )
                .toEntity( "county" )
                .addProperty( "general.stringid", "id" )
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
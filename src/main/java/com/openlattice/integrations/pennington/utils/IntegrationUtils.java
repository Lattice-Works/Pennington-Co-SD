package com.openlattice.integrations.pennington.utils;

import com.google.common.collect.Maps;
import com.openlattice.shuttle.Row;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.TimeZone;

public class IntegrationUtils {

    public static Map<County, TimeZone> getCountyToTimezoneMap() {
        Map<County, TimeZone> countyToTimezoneMap = Maps.newHashMap();
        countyToTimezoneMap.put( County.minnehaha, TimeZones.America_Denver );
        countyToTimezoneMap.put( County.pennington, TimeZones.America_Chicago);
        countyToTimezoneMap.put( County.tto, TimeZones.America_Denver);
        return countyToTimezoneMap;
    }

    public static String getInmateID( Map<String, Object> row ) {
        String inmateId = Parsers.getAsString( row.get( ZuercherConstants.INMATE_NUMBER ) );
        String partyId = Parsers.getAsString( row.get( ZuercherConstants.PARTY_ID ) );
        String finalInmateId = inmateId + "|" + partyId;
        if ( StringUtils.isNotBlank( finalInmateId ) && StringUtils.isNotBlank( finalInmateId.trim() ) ) {
            return finalInmateId;
        }
        return null;
    }

    public static String getInmateID( Row row ) {
        String inmateId = Parsers.getAsString( row.getAs( ZuercherConstants.INMATE_NUMBER ) );
        String partyId = Parsers.getAsString( row.getAs( ZuercherConstants.PARTY_ID ) );
        String finalInmateId = inmateId + "|" + partyId;
        if ( StringUtils.isNotBlank( finalInmateId ) && StringUtils.isNotBlank( finalInmateId.trim() ) ) {
            return finalInmateId;
        }
        return null;
    }

    public static JavaDateTimeHelper getDtHelperForCounty( County county, String[] dateTimePatterns ) {
        Map<County, TimeZone> countyToTimezoneMap = getCountyToTimezoneMap();
        TimeZone timezone = countyToTimezoneMap.get( county );
        return new JavaDateTimeHelper(
                timezone,
                dateTimePatterns,
                true
        );
    }


}

package com.openlattice.integrations.pennington.utils;

import com.google.common.collect.ImmutableMap;
import com.openlattice.integrations.pennington.configurations.ArrestIntegrationConfiguration;

import java.util.Map;

public class ArrestIntegrationConfigurations {

    public static final Map<County , ArrestIntegrationConfiguration> CONFIGURATIONS = ImmutableMap.of(
            County.pennington, new ArrestIntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.PENNINGTON_INCIDENT_ENTITY_SET,
                    EdmConstants.PENNINGTON_ARREST_CHARGES_ENTITY_SET,
                    EdmConstants.PENNINGTON_ARREST_PRETRIAL_CASES_ENTITY_SET,
                    EdmConstants.PENNINGTON_ADDRESS_ENTITY_SET,
                    EdmConstants.CONTACT_INFO_ENTITY_SET,
                    EdmConstants.PENNINGTON_ARRESTED_IN_ENTITY_SET,
                    EdmConstants.PENNINGTON_CHARGED_WITH_ENTITY_SET,
                    EdmConstants.PENNINGTON_APPEARS_IN_ARREST_ENTITY_SET,
                    EdmConstants.PENNINGTON_LIVES_AT_ARREST_ENTITY_SET,
                    EdmConstants.CONTACT_INFO_GIVEN_ENTITY_SET
            ),
            County.minnehaha, new ArrestIntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.MINNEHAHA_INCIDENT_ENTITY_SET,
                    EdmConstants.MINNEHAHA_ARREST_CHARGES_ENTITY_SET,
                    EdmConstants.MINNEHAHA_ARREST_PRETRIAL_CASES_ENTITY_SET,
                    EdmConstants.MINNEHAHA_ADDRESS_ENTITY_SET,
                    EdmConstants.CONTACT_INFO_ENTITY_SET,
                    EdmConstants.MINNEHAHA_ARRESTED_IN_ENTITY_SET,
                    EdmConstants.MINNEHAHA_ARREST_CHARGED_WITH_ENTITY_SET,
                    EdmConstants.MINNEHAHA_APPEARS_IN_ARREST_ENTITY_SET,
                    EdmConstants.MINNEHAHA_LIVES_AT_ARREST_ENTITY_SET,
                    EdmConstants.CONTACT_INFO_GIVEN_ENTITY_SET
            ),
            County.tto, new ArrestIntegrationConfiguration(
                    EdmConstants.TTO_PEOPLE_ENTITY_SET,
                    EdmConstants.TTO_INCIDENT_ENTITY_SET,
                    EdmConstants.TTO_ARREST_CHARGES_ENTITY_SET,
                    EdmConstants.TTO_ARREST_PRETRIAL_CASES_ENTITY_SET,
                    EdmConstants.TTO_ADDRESS_ENTITY_SET,
                    EdmConstants.TTO_CONTACT_INFO_ENTITY_SET,
                    EdmConstants.TTO_ARRESTED_IN_ENTITY_SET,
                    EdmConstants.TTO_ARREST_CHARGED_WITH_ENTITY_SET,
                    EdmConstants.TTO_APPEARS_IN_ARREST_ENTITY_SET,
                    EdmConstants.TTO_LIVES_AT_ARREST_ENTITY_SET,
                    EdmConstants.TTO_CONTACT_INFO_GIVEN_ENTITY_SET
            )
    );
}

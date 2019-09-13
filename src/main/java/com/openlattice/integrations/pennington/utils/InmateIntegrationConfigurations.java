package com.openlattice.integrations.pennington.utils;

import com.google.common.collect.ImmutableMap;
import com.openlattice.integrations.pennington.configurations.InmateIntegrationConfiguration;

import java.util.Map;

public class InmateIntegrationConfigurations {
    public static final Map<County , InmateIntegrationConfiguration> CONFIGURATIONS = ImmutableMap.of(
            County.pennington, new InmateIntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.BONDS_ENTITY_SET,
                    EdmConstants.REGISTERED_FOR_ENTITY_SET,
                    EdmConstants.BOND_SET_ENTITY_SET,
                    EdmConstants.PENNINGTON_JAIL_STAY_ENTITY_SET,
                    EdmConstants.PENNINGTON_SUBJECT_OF_ENTITY_SET
            ),
            County.minnehaha, new InmateIntegrationConfiguration(
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.BONDS_ENTITY_SET,
                    EdmConstants.REGISTERED_FOR_ENTITY_SET,
                    EdmConstants.BOND_SET_ENTITY_SET,
                    EdmConstants.MINNEHAHA_JAIL_STAY_ENTITY_SET,
                    EdmConstants.MINNEHAHA_SUBJECT_OF_ENTITY_SET
            ),
            County.tto, new InmateIntegrationConfiguration(
                    EdmConstants.TTO_PEOPLE_ENTITY_SET,
                    EdmConstants.TTO_BONDS_ENTITY_SET,
                    EdmConstants.TTO_REGISTERED_FOR_ENTITY_SET,
                    EdmConstants.TTO_BOND_SET_ENTITY_SET,
                    EdmConstants.TTO_JAIL_STAY_ENTITY_SET,
                    EdmConstants.TTO_SUBJECT_OF_ENTITY_SET
            )
    );
}

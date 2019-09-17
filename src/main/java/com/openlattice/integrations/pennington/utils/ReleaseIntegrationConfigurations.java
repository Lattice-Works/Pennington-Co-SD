package com.openlattice.integrations.pennington.utils;

import com.google.common.collect.ImmutableMap;
import com.openlattice.integrations.pennington.configurations.ReleaseIntegrationConfiguration;

import java.util.Map;

public class ReleaseIntegrationConfigurations {
    public static final Map<County , ReleaseIntegrationConfiguration> CONFIGURATIONS = ImmutableMap.of(
            County.pennington, new ReleaseIntegrationConfiguration (
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.PENNINGTON_JAIL_STAY_ENTITY_SET,
                    EdmConstants.PENNINGTON_SUBJECT_OF_ENTITY_SET
            ),
            County.minnehaha, new ReleaseIntegrationConfiguration (
                    EdmConstants.PEOPLE_ENTITY_SET,
                    EdmConstants.MINNEHAHA_JAIL_STAY_ENTITY_SET,
                    EdmConstants.MINNEHAHA_SUBJECT_OF_ENTITY_SET
            ),
            County.tto, new ReleaseIntegrationConfiguration (
                    EdmConstants.TTO_PEOPLE_ENTITY_SET,
                    EdmConstants.TTO_JAIL_STAY_ENTITY_SET,
                    EdmConstants.TTO_SUBJECT_OF_ENTITY_SET
            )
    );
}

package com.openlattice.integrations.pennington;

import com.openlattice.integrations.pennington.requests.IntegrationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class IntegrationRunner {
    private static final Logger logger = LoggerFactory.getLogger( IntegrationRunner.class );

    public static void main( String[] args ) throws InterruptedException, IOException {
        IntegrationType integrationType = null;

        try {
//            integrationType = IntegrationType.valueOf( args[ 0 ] );
            integrationType = IntegrationType.ARRESTS;
        } catch ( IllegalArgumentException e ) {
            logger.error( "A valid IntegrationType must be specified as the first argument" );
        }

        String[] requestArgs = Arrays.copyOfRange( args, 1, args.length );

        switch ( integrationType ) {


            case PSAS:
                ManualPsaIntegration.integrate( requestArgs );
                break;

            case ARRESTS:
                ZuercherArrest.integrate( requestArgs );
                break;

            case CASES:
                OdysseyCasesDailyDump.integrate( requestArgs );
                break;

            case HEARINGS:
                MinPennHearings.integrate( requestArgs );
                break;

            case INMATES:
                ZuercherInmates.integrate( requestArgs );
                break;

            default:
                break;
        }

    }
}

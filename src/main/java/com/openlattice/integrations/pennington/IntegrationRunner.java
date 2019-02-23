package com.openlattice.integrations.pennington;

import java.io.IOException;

public class IntegrationRunner {

    public static void main( String[] args ) throws InterruptedException, IOException {
        if ( args.length > 3 ) {
            ZuercherArrest.integrate( args );
        } else {
            MinPennHearings.integrate( args );
        }
    }
}

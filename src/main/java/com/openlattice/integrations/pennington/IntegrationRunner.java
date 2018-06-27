package com.openlattice.integrations.pennington;

import java.io.IOException;

public class IntegrationRunner {

    public static void main( String[] args ) throws InterruptedException, IOException {
        if ( args.length > 3 ) {
            MinPennHearings.integrate( args );
        } else {
            ZuercherArrest.integrate( args );
        }
    }
}

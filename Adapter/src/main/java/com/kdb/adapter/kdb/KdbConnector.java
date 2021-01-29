package com.kdb.adapter.kdb;

import com.kdb.adapter.messages.KdbEnvelope;
import com.kdb.adapter.utils.AdapterProperties;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class KdbConnector {

    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(KdbConnector.class.getName());

    private String kdbHost;
    private String kdbPort;
    private String kdbLogin;
    private String kdbDestination;
    private static c kdbConnection = null;
    private boolean kdbConnectionEnabled;
    public boolean connectedToKdb = false;

    public KdbConnector(AdapterProperties adapterProperties){

        try {
            if (null != kdbConnection){
                testConnection(adapterProperties,"*** Currently connected to Kdb server");
            } else {
                LOG.debug("*** Attempting to connect to Kdb server");
                kdbConnection = new c(adapterProperties.getKdbHost(), adapterProperties.getKdbPort(), adapterProperties.getKdbLogin());
            }
        } catch (Exception e) {
            LOG.error("*** Encountered constructing KdbConnector: {}", e.getMessage());
            kdbConnection = null;
        }

    }

    public void closeConnection()
    {
        try {
            kdbConnection.close();
        } catch (Exception e) {
            LOG.error("*** Encountered error in method closeConnection: {}", e.getMessage());
            kdbConnection = null;
        }
    }

    public void maintainKdbConnection(AdapterProperties adapterProperties) throws c.KException, IOException {

        if (kdbConnection != null){
            testConnection(adapterProperties,"*** Still connected to Kdb server");
        }
        else {
            LOG.debug("*** Attempting to reconnect to Kdb server");
            kdbConnection = new c(adapterProperties.getKdbHost(), adapterProperties.getKdbPort(), adapterProperties.getKdbLogin());
        }
    }

    private void testConnection(AdapterProperties adapterProperties, String infoMsg) throws c.KException, IOException {
        Object queryResult = kdbConnection.k(adapterProperties.getKdbDestination());
        if (9 == ((c.Flip)queryResult).x.length) {
            LOG.info(infoMsg);
            connectedToKdb = true;
        } else {
            connectedToKdb = false;
            kdbConnection = null;
        }
    }

    public boolean saveMessage(AdapterProperties adapterProperties, KdbEnvelope kdbEnvelope){
        try {
            if (adapterProperties.isKdbConnectionEnabled()){

                if(kdbConnection == null) {
                    maintainKdbConnection(adapterProperties);
                }

                // Merge kdbMessage data with configured destination table and method to create valid kdb syntax
                kdbConnection.ks(adapterProperties.getKdbDestinationFunction(), adapterProperties.getKdbDestination(), kdbEnvelope.toObjectArray());
                LOG.debug("*** Persisted envelope ({} messages) to kdb+", kdbEnvelope.getEnvelopeDepth());
                return true;
            }
            else {
                LOG.info("kdb connection not enabled. Check config!");
                return false;
            }
        }
        catch(Exception ex){
            LOG.error(" Problem saving message data {}", ex.toString());
            return false;
        }
    }

}

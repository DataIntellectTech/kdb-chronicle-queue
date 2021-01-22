package com.kdb.adapter.kdb;

import com.kdb.adapter.chronicle.ChronicleKdbAdapter;
import com.kdb.adapter.messages.KdbEnvelope;
import com.kdb.adapter.messages.KdbMessage;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.logging.Logger;

@Component
public class KdbConnector {

    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(KdbConnector.class.getName());

    @Value("${kdb.host}")
    private String kdbHost;

    @Value("${kdb.port}")
    private String kdbPort;

    @Value("${kdb.login}")
    private String kdbLogin;

    @Value("${kdb.destination}")
    private String kdbDestination;

    private static c kdbConnection = null;

    @Value("${kdb.connection-enabled}")
    private boolean kdbConnectionEnabled;

    public boolean connectedToKdb = false;

    @PostConstruct
    public void connectToKdbServerOnStartup() throws InterruptedException {
        while (!connectedToKdb && kdbConnectionEnabled) {
            try {
                if (null != kdbConnection){
                    testConnectionAndLogResult("*** Currently connected to Kdb server");
                } else {
                    LOG.debug("*** Attempting to connect to Kdb server");
                    kdbConnection = new c(kdbHost, Integer.parseInt(kdbPort), kdbLogin);
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                LOG.error("*** Encountered error in method connectToKdbServer: " + e.getMessage());
                kdbConnection = null;
                Thread.sleep(5000);
            }
        }
    }

    public void closeConnection()
    {
        try {
            kdbConnection.close();
        } catch (Exception e) {
            LOG.error("*** Encountered error in method closeConnection: " + e.getMessage());
            kdbConnection = null;
        }
    }

    public void maintainKdbConnection() throws InterruptedException {
        try {
            if (null != kdbConnection){
                testConnectionAndLogResult("*** Still connected to Kdb server");
            } else {
                LOG.debug("*** Attempting to reconnect to Kdb server");
                kdbConnection = new c(kdbHost, Integer.parseInt(kdbPort), kdbLogin);
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            LOG.error("*** Encountered error in method maintainConnection: " + e.getMessage());
            kdbConnection = null;
            Thread.sleep(5000);
        }
    }

    private void testConnectionAndLogResult(String infoMsg) throws c.KException, IOException, InterruptedException {
        Object queryResult = kdbConnection.k("quote");
        if (9 == ((c.Flip)queryResult).x.length) {
            LOG.debug(infoMsg);
            connectedToKdb = true;
        } else {
            connectedToKdb = false;
            kdbConnection = null;
        }
    }

    public void saveMessage(KdbMessage kdbMessage){
        try {
            if (kdbConnectionEnabled) {
                maintainKdbConnection();
                kdbConnection.ks(kdbMessage.toString());
                LOG.debug("*** Persisted message to Kdb");
            }
        }
        catch(Exception ex){
            System.out.println("Problem saving message data " + ex.toString());
        }
    }

    public void saveMessage(KdbEnvelope kdbEnvelope, String destinationTable, String kdbMethod){
        try {
            if (kdbConnectionEnabled) {

                maintainKdbConnection();

                // Merge kdbMessage data with configured destination table and method to create valid kdb syntax
                kdbConnection.ks(kdbMethod, destinationTable, kdbEnvelope.toObjectArray());

                LOG.info("*** Persisted envelope (" + kdbEnvelope.getEnvelopeDepth() + " messages) to kdb+");
            }
        }
        catch(Exception ex){
            System.out.println("Problem saving message data " + ex.toString());
        }
    }

}

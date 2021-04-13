package uk.co.aquaq.kdb.adapter.kdb;

import uk.co.aquaq.kdb.adapter.customexceptions.AdapterConfigurationException;
import uk.co.aquaq.kdb.adapter.customexceptions.KdbException;
import uk.co.aquaq.kdb.adapter.messages.KdbEnvelope;
import uk.co.aquaq.kdb.adapter.utils.AdapterProperties;
import com.kx.c;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class KdbConnector {

  private static org.slf4j.Logger log = LoggerFactory.getLogger(KdbConnector.class.getName());

  private c kdbConnection = null;
  private boolean connectedToKdb = false;

  public KdbConnector(AdapterProperties adapterProperties) throws KdbException {

    try {

      log.debug("*** Attempting to connect to Kdb server");
      connectToKdb(adapterProperties);

    } catch (KdbException ex) {
      kdbConnection = null;
      throw ex;
    }
  }

  private void connectToKdb(AdapterProperties adapterProperties) throws KdbException {
    try {
      kdbConnection =
          new c(
              adapterProperties.getKdbHost(),
              adapterProperties.getKdbPort(),
              adapterProperties.getKdbLogin());

      connectedToKdb = testConnection();
    } catch (c.KException | IOException e) {
      kdbConnection = null;
      throw new KdbException("*** Problem connecting to kdb+: " + e.getMessage());
    }
  }

  public void closeConnection() {
    try {
      kdbConnection.close();
    } catch (Exception e) {
      log.error("*** Encountered error in method closeConnection: {}", e.getMessage());
      kdbConnection = null;
    }
  }

  public void maintainKdbConnection(AdapterProperties adapterProperties)
      throws KdbException {
    try {
      if (!testConnection()) {
        log.info("*** Attempting to reconnect to Kdb+");
        connectToKdb(adapterProperties);
      }
    } catch (c.KException | IOException e) {
      kdbConnection = null;
      throw new KdbException("*** Problem maintaining connection to Kdb+: " + e.getMessage());
    }
  }

  private boolean testConnection() throws c.KException, IOException {
    Object queryResult = kdbConnection.k("1+1");
    return queryResult.toString().equals("2");
  }

  public void saveEnvelope(AdapterProperties adapterProperties, KdbEnvelope kdbEnvelope) throws KdbException, IOException, AdapterConfigurationException {
    try {
      if (adapterProperties.isKdbConnectionEnabled()) {

        if (!connectedToKdb) {
          connectToKdb(adapterProperties);
        }

        // Merge kdbMessage data with configured destination table and method to create valid kdb
        // syntax
        kdbConnection.ks(
            adapterProperties.getKdbDestinationFunction(),
            adapterProperties.getKdbDestination(),
            kdbEnvelope.toObjectArray());

        log.debug("*** Persisted envelope ({} messages) to kdb+", kdbEnvelope.getEnvelopeDepth());

      } else {
        log.info("kdb connection not enabled. Check config!");
        throw new AdapterConfigurationException("Kdb connection not enabled.");
      }
    } catch (KdbException | IOException ex) {
      log.error(" Problem saving message data {}", ex.toString());
      throw ex;
    }
  }
}

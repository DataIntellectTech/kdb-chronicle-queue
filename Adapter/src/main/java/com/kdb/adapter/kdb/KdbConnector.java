package com.kdb.adapter.kdb;

import com.kdb.adapter.messages.KdbQuoteEnvelope;
import com.kdb.adapter.utils.AdapterProperties;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class KdbConnector {

  private static org.slf4j.Logger LOG = LoggerFactory.getLogger(KdbConnector.class.getName());

  private c kdbConnection = null;
  private boolean connectedToKdb = false;

  public KdbConnector(AdapterProperties adapterProperties) {

    try {

      LOG.debug("*** Attempting to connect to Kdb server");

      kdbConnection =
          new c(
              adapterProperties.getKdbHost(),
              adapterProperties.getKdbPort(),
              adapterProperties.getKdbLogin());

      connectedToKdb = testConnection();

    } catch (Exception e) {
      LOG.error("*** Encountered constructing KdbConnector: {}", e.getMessage());
      kdbConnection = null;
    }
  }

  public void closeConnection() {
    try {
      kdbConnection.close();
    } catch (Exception e) {
      LOG.error("*** Encountered error in method closeConnection: {}", e.getMessage());
      kdbConnection = null;
    }
  }

  public void maintainKdbConnection(AdapterProperties adapterProperties)
      throws c.KException, IOException {

    connectedToKdb = testConnection();
    if (!connectedToKdb) {
      LOG.debug("*** Attempting to reconnect to Kdb server");
      kdbConnection =
          new c(
              adapterProperties.getKdbHost(),
              adapterProperties.getKdbPort(),
              adapterProperties.getKdbLogin());
    }
  }

  private boolean testConnection()
      throws c.KException, IOException {
    Object queryResult = kdbConnection.k("1+1");
    return (queryResult.toString().equals("2")) ? true : false;
  }

  public boolean saveMessage(AdapterProperties adapterProperties, KdbQuoteEnvelope kdbEnvelope) {
    try {
      if (adapterProperties.isKdbConnectionEnabled()) {

        if (!connectedToKdb) {
          maintainKdbConnection(adapterProperties);
        }

        // Merge kdbMessage data with configured destination table and method to create valid kdb
        // syntax
        kdbConnection.ks(
            adapterProperties.getKdbDestinationFunction(),
            adapterProperties.getKdbDestination(),
            kdbEnvelope.toObjectArray());
        LOG.debug("*** Persisted envelope ({} messages) to kdb+", kdbEnvelope.getEnvelopeDepth());
        return true;
      } else {
        LOG.info("kdb connection not enabled. Check config!");
        return false;
      }
    } catch (Exception ex) {
      LOG.error(" Problem saving message data {}", ex.toString());
      return false;
    }
  }
}

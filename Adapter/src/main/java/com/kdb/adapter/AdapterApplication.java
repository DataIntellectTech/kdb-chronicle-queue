package com.kdb.adapter;

import com.kdb.adapter.chronicle.ChronicleToKdbAdapter;
import com.kdb.adapter.messages.MessageTypes;
import com.kdb.adapter.utils.AdapterProperties;
import com.kdb.adapter.utils.PropertyFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.*;

public class AdapterApplication {

  private static Logger LOG = LoggerFactory.getLogger(AdapterApplication.class);

  public static void main(String[] args) {

    final ChronicleToKdbAdapter adapter = new ChronicleToKdbAdapter();

    try {

      final PropertyFileLoader properties = new PropertyFileLoader();
      final Properties props =
              properties.getPropValues(args.length > 0 ? args[1] : "application.properties");
      final AdapterProperties adapterProperties = new AdapterProperties(props);

      // Seed the adapter with the configured type...
      setAdapterMessageType(adapterProperties.getAdapterMessageType(), adapter);

      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

      Runnable task =
              () -> adapter.processMessages(adapterProperties);

      scheduler.scheduleWithFixedDelay(
          task, 0, adapterProperties.getAdapterWaitTimeWhenNoMsgs(), TimeUnit.MILLISECONDS);

    } catch (Exception ex) {
      LOG.error("Problem running Adapter: {}", ex.toString());
    }
    finally{
      adapter.tidyUp();
    }
  }

  private static void setAdapterMessageType(String messageType, ChronicleToKdbAdapter adapter) {
    // Set adapter message factory type based on config property
    if (messageType.equalsIgnoreCase("QUOTE")) {
      adapter.setMessageType(MessageTypes.AdapterMessageTypes.QUOTE);
    } else if (messageType.equalsIgnoreCase("TRADE")) {
      adapter.setMessageType(MessageTypes.AdapterMessageTypes.TRADE);
    } else {
      LOG.error("Adapter type ({}) not configured yet. Check config.", messageType);
      System.exit(-1);
    }
  }
}

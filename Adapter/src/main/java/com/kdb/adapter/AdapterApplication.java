package com.kdb.adapter;

import com.kdb.adapter.chronicle.ChronicleToKdbAdapter;
import com.kdb.adapter.messages.MessageTypes;
import com.kdb.adapter.utils.AdapterProperties;
import com.kdb.adapter.utils.PropertyFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AdapterApplication {

  private static Logger LOG = LoggerFactory.getLogger(AdapterApplication.class);

  public static void main(String[] args) {

    try {

      final ChronicleToKdbAdapter adapter = new ChronicleToKdbAdapter();

      final PropertyFileLoader properties = new PropertyFileLoader();
      final Properties props = properties.getPropValues(args.length > 0 ? args[1] : "application.properties");
      final AdapterProperties adapterProperties = new AdapterProperties(props);
      int ret = 0;

      // Set adapter message factory type based on config property
      if (adapterProperties.getAdapterMessageType().equalsIgnoreCase("QUOTE")) {
        adapter.setMessageType(MessageTypes.AdapterMessageTypes.QUOTE);
      }
      else if (adapterProperties.getAdapterMessageType().equalsIgnoreCase("TRADE")) {
        adapter.setMessageType(MessageTypes.AdapterMessageTypes.TRADE);
      }
      else {
        LOG.error(
            "Adapter type ({}) not configured yet. Check config.",
            adapterProperties.getAdapterMessageType());
        System.exit(-1);
      }

      // Keep running processMessages method of the adapter to do as it says...
      while (ret != -1) {
        ret = adapter.processMessages(adapterProperties);
        Thread.sleep(adapterProperties.getAdapterWaitTimeWhenNoMsgs());
      }

      adapter.tidyUp();
      System.exit(ret);
    } catch (Exception ex) {
      LOG.error("Problem running Adapter: {}", ex.toString());
    }
  }
}

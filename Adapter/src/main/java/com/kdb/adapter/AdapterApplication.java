package com.kdb.adapter;

import com.kdb.adapter.chronicle.ChronicleToKdbAdapter;
import com.kdb.adapter.utils.AdapterProperties;
import com.kdb.adapter.utils.PropertyFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class AdapterApplication {

  private static Logger LOG = LoggerFactory.getLogger(AdapterApplication.class);

  public static void main(String[] args) {

    try {

      final PropertyFileLoader properties = new PropertyFileLoader();
      final Properties props =
          properties.getPropValues(args.length > 0 ? args[1] : "application.properties");
      final AdapterProperties adapterProperties = new AdapterProperties(props);

      final ChronicleToKdbAdapter adapter =
          new ChronicleToKdbAdapter(adapterProperties.getAdapterMessageType(), adapterProperties);

      Thread thread = new Thread(adapter);
      thread.start();

      // TODO introduce means to call adapter.stop() from here

    } catch (Exception ex) {
      LOG.error("Problem running Adapter: Exception: {}", ex.toString());
    }
  }
}

package com.kdb.adapter;

import com.kdb.adapter.chronicle.ChronicleKdbAdapter;
import com.kdb.adapter.utils.AdapterProperties;
import com.kdb.adapter.utils.PropertyFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class AdapterApplication {

  private static Logger LOG = LoggerFactory.getLogger(AdapterApplication.class);

  public static void main(String[] args) {

    final ChronicleKdbAdapter adapter = new ChronicleKdbAdapter();

    final PropertyFileLoader properties = new PropertyFileLoader();

    try {

      // load config from  properties file
      final Properties props = properties.getPropValues(args.length > 0 ? args[1] : "application.properties");
      final AdapterProperties adapterProperties = new AdapterProperties(props);

      int ret = 0;
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

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

    ChronicleKdbAdapter adapter = new ChronicleKdbAdapter();

    PropertyFileLoader properties = new PropertyFileLoader();

    try {

      // load config from  properties file
      Properties props = properties.getPropValues("");
      AdapterProperties adapterProperties = new AdapterProperties(props);

      int ret = 0;
      while (ret != -1) {
        ret = adapter.processMessages(adapterProperties);
        try {
          Thread.sleep(adapterProperties.getAdapterWaitTimeWhenNoMsgs());
        } catch (InterruptedException ie) {
          LOG.error("Problem with sleep on no messages. Ending.");
          break;
        }
      }

      adapter.tidyUp();
      System.exit(ret);
    } catch (Exception ex) {
      LOG.error("Problem running Adapter: " + ex.toString());
    }
  }
}

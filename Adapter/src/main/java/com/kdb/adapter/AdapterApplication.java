package com.kdb.adapter;

import com.kdb.adapter.chronicle.ChronicleToKdbAdapter;
import com.kdb.adapter.utils.AdapterProperties;
import com.kdb.adapter.utils.PropertyFileLoader;
import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
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
      if (adapter.setAdapterMessageType(adapterProperties.getAdapterMessageType())) {

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        JLBH jlbh = new JLBH(new JLBHOptions());

        Runnable task = () -> adapter.processMessages(adapterProperties, jlbh);

        scheduler.scheduleWithFixedDelay(
            task, 0, adapterProperties.getAdapterWaitTimeWhenNoMsgs(), TimeUnit.MILLISECONDS);
      } else {
        LOG.info("Error setting Adapter Message Type. Shutting down.");
      }
    } catch (Exception ex) {
      LOG.error("Problem running Adapter: Exception: {}", ex.toString());
    } finally {
      adapter.tidyUp();
    }
  }
}

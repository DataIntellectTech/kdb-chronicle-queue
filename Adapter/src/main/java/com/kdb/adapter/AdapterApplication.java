package com.kdb.adapter;

import com.kdb.adapter.chronicle.ChronicleToKdbAdapter;
import com.kdb.adapter.utils.AdapterProperties;
import com.kdb.adapter.utils.PropertyFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AdapterApplication {

  private static Logger log = LoggerFactory.getLogger(AdapterApplication.class);
  private static AtomicBoolean noStopFile = new AtomicBoolean(true);

  public static void main(String[] args) {

    try {

      final PropertyFileLoader properties = new PropertyFileLoader();
      final Properties props =
          properties.getPropValues(
              args.length > 0 ? args[0] : "src\\main\\resources\\application.properties");
      final AdapterProperties adapterProperties = new AdapterProperties(props);

      final ChronicleToKdbAdapter adapter =
          new ChronicleToKdbAdapter(adapterProperties.getAdapterMessageType(), adapterProperties);

      // Check application.properties for runMode...
      // adapter.runMode=NORMAL -> normal mode processing messages on queue to kdb+
      // adapter.runMode=KDB_BENCH -> simple testing of batched (envelope) kdb+ writes only

      Thread thread = new Thread(adapter);
      thread.start();

      while (noStopFile.get()) {
        checkForStopSignal(adapterProperties, adapter);
        TimeUnit.MILLISECONDS.sleep(adapterProperties.getStopFileCheckInterval());
      }

    } catch (Exception ex) {
      log.error("Problem running Adapter: Exception: {}", ex.toString());
    }
  }

  public static void checkForStopSignal(
      AdapterProperties adapterProperties, ChronicleToKdbAdapter adapter) {
    try {
      Path path = Paths.get(adapterProperties.getStopFile());
      if (Files.exists(path)) {
        log.info("Stop file present. Stop running adapter");
        adapter.stop();
        noStopFile.set(false);
      }
    } catch (Exception ex) {
      log.debug("Stop file not found. Keep running");
    }
  }
}

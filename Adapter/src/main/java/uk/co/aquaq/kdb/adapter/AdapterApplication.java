package uk.co.aquaq.kdb.adapter;

import uk.co.aquaq.kdb.adapter.chronicle.ChronicleToKdbAdapter;
import uk.co.aquaq.kdb.adapter.utils.AdapterProperties;
import uk.co.aquaq.kdb.adapter.utils.PropertyFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AdapterApplication {

  private static final Logger log = LoggerFactory.getLogger(AdapterApplication.class);
  private static AtomicBoolean noStopFile = new AtomicBoolean(true);

  public static void main(String[] args) {

    try {

      final var properties = new PropertyFileLoader();
      final var props =
          properties.getPropValues(
              args.length > 0 ? args[0] : "src\\main\\resources\\application.properties");
      final var adapterProperties = new AdapterProperties(props);

      final var adapter =
          new ChronicleToKdbAdapter(adapterProperties.getAdapterMessageType(), adapterProperties);

      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<?> future = executor.submit(adapter);

      while (noStopFile.get()) {
        if (future.isDone()) break;
        checkForStopSignal(adapterProperties, adapter);
        TimeUnit.SECONDS.sleep(adapterProperties.getStopFileCheckInterval());
      }

      executor.shutdownNow();

    } catch (Exception ex) {
      log.error("Problem running Adapter: Exception: {}", ex.toString());
    }
  }

  public static void checkForStopSignal(
      AdapterProperties adapterProperties, ChronicleToKdbAdapter adapter) {
    try {
      var path = Paths.get(adapterProperties.getStopFile());
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

package uk.co.aquaq.kdb.adapter.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileLoader {

  private static Logger log = LoggerFactory.getLogger(PropertyFileLoader.class);

  public Properties getPropValues(String propFile) throws IOException {

    Properties prop = new Properties();

    try (InputStream inputStream = new FileInputStream(propFile)) {
      if (inputStream != null) {
        prop.load(inputStream);
      } else {
        throw new FileNotFoundException(
            "property file '" + propFile + "' not found in the classpath");
      }

    } catch (Exception e) {
      log.error("Exception: {}", e.toString());
    }
    return prop;
  }
}

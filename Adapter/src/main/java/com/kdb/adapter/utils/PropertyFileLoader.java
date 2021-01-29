package com.kdb.adapter.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileLoader {

  private static Logger LOG = LoggerFactory.getLogger(PropertyFileLoader.class);

  String result = "";
  InputStream inputStream;

  public Properties getPropValues(String propFile) throws IOException {

    Properties prop = new Properties();
    try {
      String propFileName = (propFile.equals("")) ? "application.properties" : propFile;
      inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

      if (inputStream != null) {
        prop.load(inputStream);
      } else {
        throw new FileNotFoundException(
            "property file '" + propFileName + "' not found in the classpath");
      }

    } catch (Exception e) {
      LOG.error("Exception: {}", e);
    } finally {
      try{
        inputStream.close();
      }
      catch(NullPointerException ex){}
    }
    return prop;
  }
}

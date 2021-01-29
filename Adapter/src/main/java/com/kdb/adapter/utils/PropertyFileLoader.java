package com.kdb.adapter.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

public class PropertyFileLoader {

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
      System.out.println("Exception: " + e);
    } finally {
      inputStream.close();
    }
    return prop;
  }
}

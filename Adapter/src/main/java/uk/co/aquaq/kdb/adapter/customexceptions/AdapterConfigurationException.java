package uk.co.aquaq.kdb.adapter.customexceptions;

public class AdapterConfigurationException extends RuntimeException {

  public AdapterConfigurationException(String message) {
    super(message);
  }

  public AdapterConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}

package uk.co.aquaq.kdb.adapter.customexceptions;

public class KdbException extends RuntimeException {

  public KdbException(String message) {
    super(message);
  }

  public KdbException(String message, Throwable cause) {
    super(message, cause);
  }
}

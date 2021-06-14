package uk.co.aquaq.kdb.adapter.messages;

public class MessageTypes {

  public enum AdapterMessageTypes {
    QUOTE("uk.co.aquaq.kdb.adapter.messages.KdbQuoteMessage"),
    TRADE("uk.co.aquaq.kdb.adapter.messages.KdbTradeMessage");

    public final String implClass;

    private AdapterMessageTypes(String implClass) {
      this.implClass = implClass;
    }
  }

}

package com.kdb.adapter.messages;

public class MessageTypes {

  public enum AdapterMessageTypes {
    QUOTE("com.kdb.adapter.messages.ChronicleQuoteMsg"),
    TRADE("com.kdb.adapter.messages.ChronicleTradeMsg");

    public final String implClass;

    private AdapterMessageTypes(String implClass) {
      this.implClass = implClass;
    }
  }

}

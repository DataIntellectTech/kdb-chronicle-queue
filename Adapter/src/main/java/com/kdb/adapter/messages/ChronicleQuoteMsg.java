package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class ChronicleQuoteMsg extends ChronicleMessage {

  private LocalDateTime time;
  private String sym;
  private double bid;
  private double bsize;
  private double ask;
  private double assize;
  private String bex;
  private String aex;

  public ChronicleQuoteMsg(LocalDateTime time, String sym, double bid, double bsize, double ask, double assize, String bex, String aex){
    this.time = time;
    this.sym = sym;
    this.bid = bid;
    this.bsize = bsize;
    this.ask = ask;
    this.assize = assize;
    this.bex = bex;
    this.aex = aex;
  }

  @Override
  public void readMarshallable(BytesIn bytes) {
    this.time = (LocalDateTime) bytes.readObject(LocalDateTime.class);
    this.sym = bytes.readUtf8();
    this.bid = bytes.readDouble();
    this.bsize = bytes.readDouble();
    this.ask = bytes.readDouble();
    this.assize = bytes.readDouble();
    this.bex = bytes.readUtf8();
    this.aex = bytes.readUtf8();
  }

  @Override
  public String toString() {
    // Format chronicle part of message...
    // E.g. Return value (2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)
    return String.format(
        "(%s;`%s;%s;%s;%s;%s;`%s;`%s)",
        this.time, this.sym, this.bid, this.bsize, this.ask, this.assize, this.bex, this.aex);
  }

  public static class ChronicleQuoteMsgBuilder {

    private LocalDateTime time;
    private String sym;
    private double bid;
    private double bsize;
    private double ask;
    private double assize;
    private String bex;
    private String aex;

    public ChronicleQuoteMsgBuilder withTime(LocalDateTime time) {
      this.time = time;
      return this;
    }

    public ChronicleQuoteMsgBuilder withSym(String sym) {
      this.sym = sym;
      return this;
    }

    public ChronicleQuoteMsgBuilder withBid(double bid) {
      this.bid = bid;
      return this;
    }

    public ChronicleQuoteMsgBuilder withBsize(double bsize) {
      this.bsize = bsize;
      return this;
    }

    public ChronicleQuoteMsgBuilder withAsk(double ask) {
      this.ask = ask;
      return this;
    }

    public ChronicleQuoteMsgBuilder withAssize(double assize) {
      this.assize = assize;
      return this;
    }

    public ChronicleQuoteMsgBuilder withBex(String bex) {
      this.bex = bex;
      return this;
    }

    public ChronicleQuoteMsgBuilder withAex(String aex) {
      this.aex = aex;
      return this;
    }

    public ChronicleQuoteMsg build() {
      ChronicleQuoteMsg quoteMsg = new ChronicleQuoteMsg();
      quoteMsg.time = this.time;
      quoteMsg.sym = this.sym;
      quoteMsg.bid = this.bid;
      quoteMsg.bsize = this.bsize;
      quoteMsg.ask = this.ask;
      quoteMsg.assize = this.assize;
      quoteMsg.bex = this.bex;
      quoteMsg.aex = this.aex;
      return quoteMsg;
    }
  }
}

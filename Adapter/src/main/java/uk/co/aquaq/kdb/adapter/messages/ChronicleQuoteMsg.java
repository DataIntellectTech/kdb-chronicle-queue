package uk.co.aquaq.kdb.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class ChronicleQuoteMsg extends ChronicleMessage {

  public long ts;
  private LocalDateTime time;
  private String sym;
  private double bid;
  private double bsize;
  private double ask;
  private double assize;
  private String bex;
  private String aex;

  public ChronicleQuoteMsg(final Long ts, final LocalDateTime time, final String sym, final double bid, final double bsize, final double ask, final double assize, final String bex, final String aex){
    this.ts = ts;
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
    this.ts = bytes.readLong();
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
  public void writeMarshallable(BytesOut bytes) {
    bytes.writeLong(this.ts);
    bytes.writeObject(LocalDateTime.class, this.time);
    bytes.writeUtf8(this.sym);
    bytes.writeDouble(this.bid);
    bytes.writeDouble(this.bsize);
    bytes.writeDouble(this.ask);
    bytes.writeDouble(this.assize);
    bytes.writeUtf8(this.bex);
    bytes.writeUtf8(this.aex);
  }

  @Override
  public String toString() {
    // Format chronicle part of message...
    // E.g. Return value (2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)
    return String.format(
        "(%s;`%s;%s;%s;%s;%s;`%s;`%s)",
        this.time, this.sym, this.bid, this.bsize, this.ask, this.assize, this.bex, this.aex);
  }

}

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
public class ChronicleTradeMsg extends ChronicleMessage {

  public long ts;
  private LocalDateTime time;
  private String sym;
  private double price;
  private double size;
  private String ex;

  public ChronicleTradeMsg(final Long ts, final LocalDateTime time, final String sym, final double price, final double size, final String ex){
    this.ts = ts;
    this.time = time;
    this.sym = sym;
    this.price = price;
    this.size = size;
    this.ex = ex;
  }

  @Override
  public void readMarshallable(BytesIn bytes) {
    this.ts = bytes.readLong();
    this.time = (LocalDateTime) bytes.readObject(LocalDateTime.class);
    this.sym = bytes.readUtf8();
    this.price = bytes.readDouble();
    this.size = bytes.readDouble();
    this.ex = bytes.readUtf8();
  }

  @Override
  public void writeMarshallable(BytesOut bytes) {
    bytes.writeLong(this.ts);
    bytes.writeObject(LocalDateTime.class, this.time);
    bytes.writeUtf8(this.sym);
    bytes.writeDouble(this.price);
    bytes.writeDouble(this.size);
    bytes.writeUtf8(this.ex);
  }

  @Override
  public String toString() {
    // Format chronicle part of message...
    return String.format(
        "(%s;`%s;%s;%s;`%s)",
        this.time, this.sym, this.price, this.size, this.ex);
  }

}

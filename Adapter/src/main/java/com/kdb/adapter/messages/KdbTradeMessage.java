package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class KdbTradeMessage implements KdbMessage {

  public long ts;
  private LocalDateTime time;
  private String sym;
  private double price;
  private double size;
  private String ex;

  public KdbTradeMessage(final Long ts, final LocalDateTime time, final String sym, final double price, final double size, final String ex){
    this.ts = ts;
    this.time = time;
    this.sym = sym;
    this.price = price;
    this.size = size;
    this.ex = ex;
  }

  @Override
  public String toString() {
    // Format chronicle part of message...
    return String.format(
            "(%s;`%s;%s;%s;`%s)",
            this.time, this.sym, this.price, this.size, this.ex);
  }

  public Object[] toObjectArray() {

    Timestamp[] chrontimeArr = new Timestamp[] {Timestamp.valueOf(this.time)};
    String[] symArr = new String[] {this.sym};
    double[] priceArr = new double[] {this.price};
    double[] sizeArr = new double[] {this.size};
    String[] exArr = new String[] {this.ex};

    return new Object[] {chrontimeArr, symArr, priceArr, sizeArr, exArr};
  }

}

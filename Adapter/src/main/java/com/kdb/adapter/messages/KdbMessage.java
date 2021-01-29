package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.sql.Timestamp;
import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class KdbMessage implements AdapterMessage {

  private LocalDateTime time;
  private String sym;
  private double bid;
  private double bsize;
  private double ask;
  private double assize;
  private String bex;
  private String aex;

  @Override
  public String toString() {
    // Change to just return formatted kdb data
    // e.g. (2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)
    return String.format(
        "(%s;%s;%s;%s;%s;%s;%s;%s)",
        this.time, this.sym, this.bid, this.bsize, this.ask, this.assize, this.bex, this.aex);
  }

  public Object[] toObjectArray() {

    Timestamp[] chrontime = new Timestamp[] {Timestamp.valueOf(this.time)};
    String[] sym = new String[] {this.sym};
    double[] bid = new double[] {this.bid};
    double[] bSize = new double[] {this.bsize};
    double[] ask = new double[] {this.ask};
    double[] aSize = new double[] {this.assize};
    String[] bex = new String[] {this.bex};
    String[] aex = new String[] {this.aex};

    Object[] data = new Object[] {chrontime, sym, bid, bSize, ask, aSize, bex, aex};

    return data;
  }
}

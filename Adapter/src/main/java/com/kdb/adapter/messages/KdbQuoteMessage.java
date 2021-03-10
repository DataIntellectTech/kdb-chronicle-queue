package com.kdb.adapter.messages;

import com.kdb.adapter.mapper.QuoteMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.mapstruct.factory.Mappers;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class KdbQuoteMessage implements KdbMessage {

  private long ts;
  private LocalDateTime time;
  private String sym;
  private double bid;
  private double bsize;
  private double ask;
  private double assize;
  private String bex;
  private String aex;

  public KdbQuoteMessage(final Long ts, final LocalDateTime time, final String sym, final double bid, final double bsize, final double ask, final double assize, final String bex, final String aex){
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
  public String toString() {
    // Change to just return formatted kdb data
    // e.g. (2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)
    return String.format(
        "(%s;%s;%s;%s;%s;%s;%s;%s)",
        this.time, this.sym, this.bid, this.bsize, this.ask, this.assize, this.bex, this.aex);
  }

  public Object[] toObjectArray() {

    Timestamp[] chrontimeArr = new Timestamp[] {Timestamp.valueOf(this.time)};
    String[] symArr = new String[] {this.sym};
    double[] bidArr = new double[] {this.bid};
    double[] bSizeArr = new double[] {this.bsize};
    double[] askArr = new double[] {this.ask};
    double[] aSizeArr = new double[] {this.assize};
    String[] bexArr = new String[] {this.bex};
    String[] aexArr = new String[] {this.aex};

    return new Object[] {chrontimeArr, symArr, bidArr, bSizeArr, askArr, aSizeArr, bexArr, aexArr};
  }

}

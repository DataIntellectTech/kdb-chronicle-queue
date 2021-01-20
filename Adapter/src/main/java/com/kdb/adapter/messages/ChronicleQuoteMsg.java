package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class ChronicleQuoteMsg implements AdapterMessage{

    private LocalDateTime time;
    private String sym;
    private double bid;
    private double bsize;
    private double ask;
    private double assize;
    private String bex;
    private String aex;

    public ChronicleQuoteMsg(LocalDateTime time, String sym, double bid, double bsize, double ask, double assize, String bex, String aex) {
        this.time = time;
        this.sym = sym;
        this.bid = bid;
        this.bsize = bsize;
        this.ask = ask;
        this.assize = assize;
        this.bex = bex;
        this.aex = aex;
    }

    public String toString(){
        //Format chronicle part of message...
        //E.g. Return value (2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)
        return String.format("(%s;`%s;%s;%s;%s;%s;`%s;`%s)", this.time, this.sym, this.bid, this.bsize, this.ask, this.assize, this.bex, this.aex);
    }

}

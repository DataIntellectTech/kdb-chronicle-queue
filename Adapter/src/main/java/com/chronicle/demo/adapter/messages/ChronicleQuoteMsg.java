package com.chronicle.demo.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ChronicleQuoteMsg implements AdapterMessage{

    public ChronicleQuoteMsg(String time, String sym, String bid, String bsize, String ask, String assize, String bex, String aex) {
        this.time = time;
        this.sym = sym;
        this.bid = bid + 'f';
        this.bsize = bsize + 'f';
        this.ask = ask + 'f';
        this.assize = assize + 'f';
        this.bex = bex;
        this.aex = aex;
    }

    public String toString(){
        //Format chronicle part of message...
        //E.g. Return value (2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)
        return String.format("(%s;`%s;%s;%s;%s;%s;`%s;`%s)", this.time, this.sym, this.bid, this.bsize, this.ask, this.assize, this.bex, this.aex);
    }

    private String time;
    private String sym;
    private String bid;
    private String bsize;
    private String ask;
    private String assize;
    private String bex;
    private String aex;

}

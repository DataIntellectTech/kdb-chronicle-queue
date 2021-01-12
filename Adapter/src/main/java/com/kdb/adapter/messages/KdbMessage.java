package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class KdbMessage implements AdapterMessage {

    private String time;
    private String sym;
    private String bid;
    private String bsize;
    private String ask;
    private String assize;
    private String bex;
    private String aex;

    @Override
    public String toString() {
        //Change to just return formatted kdb data
        //e.g. (2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)
        return String.format("(%s;`%s;%s;%s;%s;%s;`%s;`%s)", this.time, this.sym, this.bid, this.bsize, this.ask, this.assize, this.bex, this.aex);
    }
}

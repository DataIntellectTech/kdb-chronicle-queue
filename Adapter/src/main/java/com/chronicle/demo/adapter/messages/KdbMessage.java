package com.chronicle.demo.adapter.messages;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class KdbMessage implements AdapterMessage {

    private String rawMessage;
    private String kdbMethod;
    private String kdbTable;

    public KdbMessage(String rawMessage, String kdbDest, String kdbMethod){
        this.rawMessage = rawMessage;
        this.kdbTable = kdbDest;
        this.kdbMethod = kdbMethod;
    }

    @Override
    public String toString() {
        //Old E.g. return value value(`upd;`quote;(2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS))
        //return String.format("value(`%s;`%s;%s)", this.kdbMethod, this.kdbTable, this.rawMessage);
        //New E.g. return value value(`u.upd[`quote;(2020.12.01+15:06:27.333Z;`HEIN.AS;100;9014;100;24543;`XAMS;`XAMS)])
        return String.format("value(`%s[`%s;%s])", this.kdbMethod, this.kdbTable, this.rawMessage);
    }
}

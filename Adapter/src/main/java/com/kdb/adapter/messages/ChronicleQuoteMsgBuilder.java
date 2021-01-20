package com.kdb.adapter.messages;

import java.time.LocalDateTime;

public class ChronicleQuoteMsgBuilder{

    private LocalDateTime time;
    private String sym;
    private double bid;
    private double bsize;
    private double ask;
    private double assize;
    private String bex;
    private String aex;

    public ChronicleQuoteMsgBuilder()
    {
    }

    public ChronicleQuoteMsgBuilder(LocalDateTime time, String sym, double bid, double bsize, double ask, double assize, String bex, String aex) {
        this.time = time;
        this.sym = sym;
        this.bid = bid;
        this.bsize = bsize;
        this.ask = ask;
        this.assize = assize;
        this.bex = bex;
        this.aex = aex;
    }

    public ChronicleQuoteMsgBuilder setTime(LocalDateTime time) {
        this.time = time;
        return this;
    }

    public ChronicleQuoteMsgBuilder setSym(String sym) {
        this.sym = sym;
        return this;
    }

    public ChronicleQuoteMsgBuilder setBid(double bid) {
        this.bid = bid;
        return this;
    }

    public ChronicleQuoteMsgBuilder setBsize(double bsize) {
        this.bsize = bsize;
        return this;
    }

    public ChronicleQuoteMsgBuilder setAsk(double ask) {
        this.ask = ask;
        return this;
    }

    public ChronicleQuoteMsgBuilder setAssize(double assize) {
        this.assize = assize;
        return this;
    }

    public ChronicleQuoteMsgBuilder setBex(String bex) {
        this.bex = bex;
        return this;
    }

    public ChronicleQuoteMsgBuilder setAex(String aex) {
        this.aex = aex;
        return this;
    }

    public ChronicleQuoteMsg build() {
        return new ChronicleQuoteMsg(time,sym,bid,bsize,ask,assize,bex,aex);
    }

}

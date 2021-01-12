package com.kdb.adapter.messages;

public class ChronicleQuoteMsgBuilder{

    private String time;
    private String sym;
    private String bid;
    private String bsize;
    private String ask;
    private String assize;
    private String bex;
    private String aex;

    public ChronicleQuoteMsgBuilder()
    {

    }

    public ChronicleQuoteMsgBuilder(String time, String sym, String bid, String bsize, String ask, String assize, String bex, String aex) {
        this.time = time;
        this.sym = sym;
        this.bid = bid + 'f';
        this.bsize = bsize + 'f';
        this.ask = ask + 'f';
        this.assize = assize + 'f';
        this.bex = bex;
        this.aex = aex;
    }

    public ChronicleQuoteMsgBuilder setTime(String time) {
        this.time = time;
        return this;
    }

    public ChronicleQuoteMsgBuilder setSym(String sym) {
        this.sym = sym;
        return this;
    }

    public ChronicleQuoteMsgBuilder setBid(String bid) {
        this.bid = bid;
        return this;
    }

    public ChronicleQuoteMsgBuilder setBsize(String bsize) {
        this.bsize = bsize;
        return this;
    }

    public ChronicleQuoteMsgBuilder setAsk(String ask) {
        this.ask = ask;
        return this;
    }

    public ChronicleQuoteMsgBuilder setAssize(String assize) {
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

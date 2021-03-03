package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

@Getter
@Setter
public class KdbQuoteEnvelope extends KdbEnvelope<KdbQuoteMessage> {

//  private Object[] envelope;
//  private long firstIndex;
//  private long[] ts;
  private Timestamp[] chrontime;
  private String[] sym;
  private double[] bid;
  private double[] bSize;
  private double[] ask;
  private double[] aSize;
  private String[] bex;
  private String[] aex;

  private static Logger LOG = LoggerFactory.getLogger(KdbQuoteEnvelope.class);

  //Only ever use this in benchmarking route....
  public KdbQuoteEnvelope(KdbQuoteEnvelope tempEnvelope){
    envelope = new Object[] {};
    envelopeDepth = tempEnvelope.envelopeDepth;
    envelopeMaxSize = tempEnvelope.getEnvelopeMaxSize();
    full = false;
    firstIndex = -1L;
    ts = tempEnvelope.getTs();
    chrontime = tempEnvelope.getChrontime();
    sym = tempEnvelope.getSym();
    bid = tempEnvelope.getBid();
    bSize = tempEnvelope.getBSize();
    ask = tempEnvelope.getAsk();
    aSize = tempEnvelope.getASize();
    bex = tempEnvelope.getBex();
    aex = tempEnvelope.getAex();
  }

  public KdbQuoteEnvelope(int maxSize) {
    envelope = new Object[] {};
    envelopeDepth = 0;
    envelopeMaxSize = maxSize;
    full = false;
    firstIndex = -1L;
    ts = new long[] {};
    chrontime = new Timestamp[] {};
    sym = new String[] {};
    bid = new double[] {};
    bSize = new double[] {};
    ask = new double[] {};
    aSize = new double[] {};
    bex = new String[] {};
    aex = new String[] {};
  }

  @Override
  public void reset() {
    envelope = null;
    ts = null;
    chrontime = null;
    sym = null;
    bid = null;
    bSize = null;
    ask = null;
    aSize = null;
    bex = null;
    aex = null;

    envelope = new Object[] {};
    envelopeDepth = 0;
    full = false;
    firstIndex = -1L;
    ts = new long[] {};
    chrontime = new Timestamp[] {};
    sym = new String[] {};
    bid = new double[] {};
    bSize = new double[] {};
    ask = new double[] {};
    aSize = new double[] {};
    bex = new String[] {};
    aex = new String[] {};
  }

  @Override
  public void addToEnvelope(KdbQuoteMessage kdbQuoteMessage, Long index) {

    ts = addElement(ts, kdbQuoteMessage.getTs());
    chrontime = addElement(chrontime, Timestamp.valueOf(kdbQuoteMessage.getTime()));
    sym = addElement(sym, kdbQuoteMessage.getSym());
    bid = addElement(bid, kdbQuoteMessage.getBid());
    bSize = addElement(bSize, kdbQuoteMessage.getBsize());
    ask = addElement(ask, kdbQuoteMessage.getAsk());
    aSize = addElement(aSize, kdbQuoteMessage.getAssize());
    bex = addElement(bex, kdbQuoteMessage.getBex());
    aex = addElement(aex, kdbQuoteMessage.getAex());

    if (firstIndex == -1L) {
      firstIndex = index;
    }
    envelopeDepth++;
    full = envelopeDepth == envelopeMaxSize;
  }

  @Override
  public Object[] toObjectArray() {
    long start = System.nanoTime();
    envelope = new Object[] {chrontime, sym, bid, bSize, ask, aSize, bex, aex};
    long finish = System.nanoTime() - start;
    LOG.trace("TIMING: kdbEnvelope.toObjectArray() {} seconds", finish / 1e9);
    return envelope;
  }
}

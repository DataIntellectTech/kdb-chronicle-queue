package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

@Getter
@Setter
public class KdbTradeEnvelope extends KdbEnvelope<KdbTradeMessage> {

  private Timestamp[] chrontime;
  private String[] sym;
  private double[] price;
  private double[] size;
  private String[] ex;

  private static Logger log = LoggerFactory.getLogger(KdbTradeEnvelope.class);

  // Only ever use this in benchmarking route....
  public KdbTradeEnvelope(KdbTradeEnvelope tempEnvelope) {
    envelope = new Object[] {};
    envelopeDepth = tempEnvelope.envelopeDepth;
    envelopeMaxSize = tempEnvelope.getEnvelopeMaxSize();
    full = false;
    empty = true;
    firstIndex = -1L;
    ts = tempEnvelope.getTs();
    chrontime = tempEnvelope.getChrontime();
    sym = tempEnvelope.getSym();
    price = tempEnvelope.getPrice();
    size = tempEnvelope.getSize();
    ex = tempEnvelope.getEx();
  }

  public KdbTradeEnvelope(int maxSize) {
    envelope = new Object[] {};
    envelopeDepth = 0;
    envelopeMaxSize = maxSize;
    full = false;
    empty = true;
    firstIndex = -1L;
    ts = new long[] {};
    chrontime = new Timestamp[] {};
    sym = new String[] {};
    price = new double[] {};
    size = new double[] {};
    ex = new String[] {};
  }

  @Override
  public void reset() {
    envelope = null;
    ts = null;
    chrontime = null;
    sym = null;
    price = null;
    size = null;
    ex = null;

    envelope = new Object[] {};
    envelopeDepth = 0;
    full = false;
    empty = true;
    firstIndex = -1L;
    ts = new long[] {};
    chrontime = new Timestamp[] {};
    sym = new String[] {};
    price = new double[] {};
    size = new double[] {};
    ex = new String[] {};
  }

  @Override
  public void addToEnvelope(KdbTradeMessage kdbTradeMessage, Long index) {

    ts = addElement(ts, kdbTradeMessage.getTs());
    chrontime = addElement(chrontime, Timestamp.valueOf(kdbTradeMessage.getTime()));
    sym = addElement(sym, kdbTradeMessage.getSym());
    price = addElement(price, kdbTradeMessage.getPrice());
    size = addElement(size, kdbTradeMessage.getSize());
    ex = addElement(ex, kdbTradeMessage.getEx());

    if (firstIndex == -1L) {
      firstIndex = index;
    }
    envelopeDepth++;
    empty = false;
    full = envelopeDepth == envelopeMaxSize;
  }

  @Override
  public Object[] toObjectArray() {
    long start = System.nanoTime();
    envelope = new Object[] {chrontime, sym, price, size, ex};
    long finish = System.nanoTime() - start;
    log.trace("TIMING: kdbEnvelope.toObjectArray() {} seconds", finish / 1e9);
    return envelope;
  }
}

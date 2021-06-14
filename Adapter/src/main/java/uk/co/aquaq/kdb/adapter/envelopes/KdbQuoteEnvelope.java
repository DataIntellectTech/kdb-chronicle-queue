package uk.co.aquaq.kdb.adapter.envelopes;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.aquaq.kdb.adapter.customexceptions.AdapterConfigurationException;
import uk.co.aquaq.kdb.adapter.messages.*;
import uk.co.aquaq.kdb.adapter.utils.AdapterProperties;

import java.lang.reflect.Field;
import java.sql.Timestamp;

@Getter
@Setter
public class KdbQuoteEnvelope extends KdbEnvelope<KdbQuoteMessage> {

  private Timestamp[] chrontime;
  private String[] sym;
  private double[] bid;
  private double[] bSize;
  private double[] ask;
  private double[] aSize;
  private String[] bex;
  private String[] aex;

  private static Logger log = LoggerFactory.getLogger(KdbQuoteEnvelope.class);

  // Only ever use this in benchmarking route....
  public KdbQuoteEnvelope(KdbQuoteEnvelope tempEnvelope) {
    envelope = new Object[] {};
    envelopeDepth = tempEnvelope.envelopeDepth;
    envelopeMaxSize = tempEnvelope.getEnvelopeMaxSize();
    full = false;
    empty = true;
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
    empty = true;
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
    empty = true;
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
  public void addToEnvelope(KdbQuoteMessage kdbQuoteMessage, Long index, AdapterProperties props) {

    if (acceptMessage(kdbQuoteMessage, props)) {

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
      empty = false;
      full = envelopeDepth == envelopeMaxSize;
    }
  }

  @Override
  public Object[] toObjectArray() {
    long start = System.nanoTime();
    envelope = new Object[] {chrontime, sym, bid, bSize, ask, aSize, bex, aex};
    long finish = System.nanoTime() - start;
    log.trace("TIMING: kdbEnvelope.toObjectArray() {} seconds", finish / 1e9);
    return envelope;
  }

  @Override
  public boolean acceptMessage(KdbQuoteMessage kdbQuoteMessage, AdapterProperties props)
      throws AdapterConfigurationException {

    int result =
        filterMessage(
            kdbQuoteMessage,
            props.getAdapterMessageFilterField(),
            props.getAdapterMessageFilterIn(),
            props.getAdapterMessageFilterOut());

    if (result == -1)
      throw new AdapterConfigurationException("Problem filtering messages. Check config.");

    return result == 1;
  }

  public int filterMessage(
      KdbQuoteMessage msg, String filterField, String filterIn, String filterOut) {

    try {

      // Keep = 1
      // Reject = 0
      // Error = -1

      // If doesn't match filter return 0
      // If no active filter return 1
      // If matches filter return 1
      // If invalid return -1

      int ret = 0;
      boolean decisionMade = false;

      if (filterIn.length() > 0) {

        decisionMade = true;
        ret =
            filterMessageBasedOnStringField(
                MessageTypes.AdapterMessageTypes.QUOTE.implClass, true, filterField, msg, filterIn);

      } else {
        ret = 1;
      }

      if (filterOut.length() > 0) {

        ret =
            filterMessageBasedOnStringField(
                MessageTypes.AdapterMessageTypes.QUOTE.implClass,
                false,
                filterField,
                msg,
                filterOut);

      } else {
        if (!decisionMade) ret = 1;
      }
      return ret;
    } catch (Exception ex) {
      return -1;
    }
  }

  // Keep = 1
  // Reject = 0
  // Error = -1
  // filterIn = true -> if string check true then keep, else reject
  // filterIn = false -> if string check true then reject, else keep
  private int filterMessageBasedOnStringField(
      String className, boolean filterIn, String filterField, KdbQuoteMessage msg, String filter) {
    try {
      Class<?> msgClass = Class.forName(className);
      Field field = msgClass.getDeclaredField(filterField);
      field.setAccessible(true);
      String filterFieldValue = (String) field.get(msg);

      int ret = filterIn ? 0 : 1;
      if (filter.contains(filterFieldValue)) {
        ret = filterIn ? 1 : 0;
      }
      return ret;
    } catch (Exception ex) {
      return -1;
    }
  }
}

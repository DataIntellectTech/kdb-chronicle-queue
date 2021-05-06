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
  public void addToEnvelope(KdbTradeMessage kdbTradeMessage, Long index, AdapterProperties props) {

    if (acceptMessage(kdbTradeMessage, props)) {

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
  }

  @Override
  public Object[] toObjectArray() {
    long start = System.nanoTime();
    envelope = new Object[] {chrontime, sym, price, size, ex};
    long finish = System.nanoTime() - start;
    log.trace("TIMING: kdbEnvelope.toObjectArray() {} seconds", finish / 1e9);
    return envelope;
  }

  @Override
  public boolean acceptMessage(KdbTradeMessage kdbTradeMessage, AdapterProperties props)
          throws AdapterConfigurationException {

    int result =
            filterMessage(
                    kdbTradeMessage,
                    props.getAdapterMessageFilterField(),
                    props.getAdapterMessageFilterIn(),
                    props.getAdapterMessageFilterOut());

    if (result == -1)
      throw new AdapterConfigurationException("Problem filtering messages. Check config.");

    return result == 1;
  }

  public int filterMessage(
          KdbTradeMessage msg, String filterField, String filterIn, String filterOut) {

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
                        MessageTypes.AdapterMessageTypes.TRADE.implClass, true, filterField, msg, filterIn);

      } else {
        ret = 1;
      }

      if (filterOut.length() > 0) {

        ret =
                filterMessageBasedOnStringField(
                        MessageTypes.AdapterMessageTypes.TRADE.implClass,
                        false,
                        filterField,
                        msg,
                        filterOut);

      } else {
        if (!decisionMade) ret = 1;
      }
      return ret;
    } catch (Exception e) {
      return -1;
    }
  }

  // Keep = 1
  // Reject = 0
  // Error = -1
  // filterIn = true -> if string check true then keep, else reject
  // filterIn = false -> if string check true then reject, else keep
  private int filterMessageBasedOnStringField(
          String className, boolean filterIn, String filterField, KdbTradeMessage msg, String filter) {
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
    } catch (Exception e) {
      return -1;
    }
  }

}

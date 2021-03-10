package com.kdb.adapter.messages;

import lombok.Getter;
import lombok.Setter;
import java.sql.Timestamp;
import java.util.Arrays;

@Getter
@Setter
public abstract class KdbEnvelope<T> {

  Object[] envelope;
  int envelopeDepth;
  int envelopeMaxSize;
  boolean full;
  boolean empty;
  long firstIndex;
  // Needed for benchmarking
  long[] ts;

  public abstract void reset();

  public long[] addElement(long[] srcArray, long elementToAdd) {
    long[] destArray = Arrays.copyOf(srcArray, srcArray.length + 1);
    destArray[destArray.length - 1] = elementToAdd;
    return destArray;
  }

  public Timestamp[] addElement(Timestamp[] srcArray, Timestamp elementToAdd) {
    Timestamp[] destArray = Arrays.copyOf(srcArray, srcArray.length + 1);
    destArray[destArray.length - 1] = elementToAdd;
    return destArray;
  }

  public String[] addElement(String[] srcArray, String elementToAdd) {
    String[] destArray = Arrays.copyOf(srcArray, srcArray.length + 1);
    destArray[destArray.length - 1] = elementToAdd;
    return destArray;
  }

  public double[] addElement(double[] srcArray, double elementToAdd) {
    double[] destArray = Arrays.copyOf(srcArray, srcArray.length + 1);
    destArray[destArray.length - 1] = elementToAdd;
    return destArray;
  }

  public abstract void addToEnvelope(T kdbMessage, Long index);

  public abstract Object[] toObjectArray();
}

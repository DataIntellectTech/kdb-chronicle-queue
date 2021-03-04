package com.kdb.adapter.utils;

import lombok.Getter;
import lombok.Setter;
import java.util.Properties;

@Getter
@Setter
public class AdapterProperties {

  private String runMode;
  private String chronicleSource;
  private String adapterTailerName;
  private String adapterMessageType;
  private String adapterMessageFilter;
  private Long adapterWaitTimeWhenNoMsgs;
  private String kdbHost;
  private int kdbPort;
  private String kdbLogin;
  @Getter private boolean kdbConnectionEnabled;
  private String kdbDestination;
  private String kdbDestinationFunction;
  private int kdbEnvelopeSize;

  public AdapterProperties(Properties props) {

    setRunMode(props.getProperty("adapter.runMode", "NORMAL"));
    setChronicleSource(props.getProperty("chronicle.source"));
    setAdapterTailerName(props.getProperty("chronicle.tailerName"));
    setAdapterMessageType(props.getProperty("adapter.messageType"));
    setAdapterMessageFilter(props.getProperty("adapter.messageFilter", ""));
    setAdapterWaitTimeWhenNoMsgs(
        Long.parseLong(props.getProperty("adapter.waitTime.whenNoMsgs", "10000")));
    setKdbHost(props.getProperty("kdb.host", "localhost"));
    setKdbPort(Integer.parseInt(props.getProperty("kdb.port", "5000")));
    setKdbLogin(props.getProperty("kdb.login", "username:password"));
    setKdbConnectionEnabled(
        Boolean.parseBoolean(props.getProperty("kdb.connection.enabled", "true")));
    setKdbDestination(props.getProperty("kdb.destination"));
    setKdbDestinationFunction(props.getProperty("kdb.destination.function"));
    setKdbEnvelopeSize(Integer.parseInt(props.getProperty("kdb.envelope.size", "100")));
  }

  public AdapterProperties() {}
}

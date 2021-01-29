package com.kdb.adapter.utils;

import lombok.Getter;
import lombok.Setter;
import java.util.Properties;

@Getter
@Setter
public class AdapterProperties {

  private String chronicleSource;
  private String adapterTailerName;
  private String adapterMessageType;
  private Long adapterWaitTimeWhenNoMsgs;
  private String kdbHost;
  private int kdbPort;
  private String kdbLogin;
  @Getter private boolean kdbConnectionEnabled;
  private String kdbDestination;
  private String kdbDestinationFunction;
  private int kdbEnvelopeSize;
  private long kdbEnvelopeWaitTime;

  public AdapterProperties(Properties props) {

    setChronicleSource(props.getProperty("chronicle.source"));
    setAdapterTailerName(props.getProperty("adapter.tailerName"));
    setAdapterMessageType(props.getProperty("adapter.messageType"));
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
    setKdbEnvelopeWaitTime(Long.parseLong(props.getProperty("kdb.envelope.waitTime", "200")));
  }

  public AdapterProperties() {}
}

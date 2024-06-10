package uk.co.aquaq.kdb.adapter.utils;

import lombok.Getter;
import lombok.Setter;
import java.util.Properties;

@Getter
@Setter
public class AdapterProperties {

  private String runMode;
  private String stopFile;
  private long stopFileCheckInterval;
  private int coreAffinity;
  private String chronicleSource;
  private String adapterTailerName;
  private String adapterSentTailerName;
  private String adapterMessageType;
  private String adapterMessageFilterField;
  private String adapterMessageFilterIn;
  private String adapterMessageFilterOut;
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
    setStopFile(props.getProperty("adapter.stopFile", "STOP.txt"));
    setStopFileCheckInterval(Integer.parseInt(props.getProperty("adapter.stopFile.checkInterval", "5")));
    setCoreAffinity(Integer.parseInt(props.getProperty("adapter.coreAffinity", "-1")));
    setChronicleSource(props.getProperty("chronicle.source"));
    setAdapterTailerName(props.getProperty("chronicle.tailerName"));
    setAdapterSentTailerName(props.getProperty("chronicle.sentTailerName"));
    setAdapterMessageType(props.getProperty("adapter.messageType"));
    setAdapterMessageFilterField(props.getProperty("adapter.messageFilterField", ""));
    setAdapterMessageFilterIn(props.getProperty("adapter.messageFilterIn", ""));
    setAdapterMessageFilterOut(props.getProperty("adapter.messageFilterOut", ""));
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

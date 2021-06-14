package uk.co.aquaq.kdb.adapter.chronicle;

import uk.co.aquaq.kdb.adapter.customexceptions.AdapterConfigurationException;
import uk.co.aquaq.kdb.adapter.customexceptions.KdbException;
import uk.co.aquaq.kdb.adapter.envelopes.KdbEnvelope;
import uk.co.aquaq.kdb.adapter.messages.*;
import uk.co.aquaq.kdb.adapter.data.QuoteHelper;
import uk.co.aquaq.kdb.adapter.factory.AdapterFactory;
import uk.co.aquaq.kdb.adapter.kdb.KdbConnector;
import uk.co.aquaq.kdb.adapter.utils.AdapterProperties;
import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChronicleToKdbAdapter implements Runnable {

  private KdbConnector kdbConnector;
  private static final Logger log = LoggerFactory.getLogger(ChronicleToKdbAdapter.class);
  private MessageTypes.AdapterMessageTypes messageType;
  private long howManyStored = 0L;

  private AtomicBoolean stopped = new AtomicBoolean(false);
  private AdapterProperties adapterProperties;
  private JLBH jlbh;
  private long lastWriteNanos = 0;
  private static final long MIN_SEND_PAUSE_NANOS = 0;
  private NanoSampler writeToKDBSampler;
  private static final String FAILED_TO_SAVE = "Failed to save current envelope.";

  public ChronicleToKdbAdapter() {
    // Empty no args constructor
  }

  public ChronicleToKdbAdapter(MessageTypes.AdapterMessageTypes type) {
    this.setMessageType(type);
  }

  public ChronicleToKdbAdapter(String adapterMessageType, AdapterProperties props) {
    this.setAdapterMessageType(adapterMessageType);
    this.setAdapterProperties(props);
    if (adapterProperties.getCoreAffinity() > -1)
      Affinity.setAffinity(adapterProperties.getCoreAffinity());
  }

  public ChronicleToKdbAdapter(String adapterMessageType, AdapterProperties props, JLBH jlbh) {
    // JLBH for benchmarking
    this(adapterMessageType, props);
    this.setJLBH(jlbh);
  }

  public void stop() {
    stopped.set(true);
  }

  public void run() {
    // Check application.properties for runMode...
    // adapter.runMode=NORMAL -> normal mode processing messages on queue to kdb+
    // adapter.runMode=BENCH -> benchmarking mode processing messages on queue to kdb+
    // adapter.runMode=KDB_BENCH -> simple testing of batched (envelope) kdb+ writes only

    if (adapterProperties.getRunMode().equalsIgnoreCase("KDB_BENCH")) {
      benchmarkKdbOnly();
    } else if (adapterProperties.getRunMode().equalsIgnoreCase("BENCH")) {
      benchmarkProcessMessages();
    } else {
      processMessages();
    }
  }

  public void setAdapterProperties(AdapterProperties props) {
    this.adapterProperties = props;
  }

  public void setMessageType(MessageTypes.AdapterMessageTypes msgType) {
    this.messageType = msgType;
  }

  public void setJLBH(JLBH jlbh) {
    this.jlbh = jlbh;
  }

  public MessageTypes.AdapterMessageTypes getMessageType() {
    return this.messageType;
  }

  public void setAdapterMessageType(String messageType) throws AdapterConfigurationException {
    // Set adapter message factory type based on config property
    if (messageType.equalsIgnoreCase("QUOTE")) {
      this.setMessageType(MessageTypes.AdapterMessageTypes.QUOTE);
    } else if (messageType.equalsIgnoreCase("TRADE")) {
      this.setMessageType(MessageTypes.AdapterMessageTypes.TRADE);
    } else {
      log.error("Adapter type ({}) not configured yet. Check config.", messageType);
      throw new AdapterConfigurationException(
          "Adapter type (" + messageType + ") not configured yet. Check config.");
    }
  }

  public void tidyUp() {

    try {
      if (kdbConnector != null) {
        kdbConnector.closeConnection();
      }
    } catch (Exception e) {
      log.error("Exception can be ignored here..{}", e.toString());
    }
    log.debug("Resources cleaned up");
  }

  public void processMessages() {

    long tailerIndex;
    final long processStart = System.nanoTime();
    long processFinish;
    KdbEnvelope<KdbMessage> envelope;

    // 1. Connect to Chronicle Queue source
    // 2. Create "tailer" to listen for messages
    try (SingleChronicleQueue sourceQueue =
            SingleChronicleQueueBuilder.binary(adapterProperties.getChronicleSource()).build();
        ExcerptTailer tailer = sourceQueue.createTailer(adapterProperties.getAdapterTailerName())) {

      log.info("Starting Chronicle kdb Adapter");

      // Check last index read / starting index
      tailerIndex = tailer.index();
      log.info("Tailer starting at index: {}", tailerIndex);

      // Use AdapterFactory to return correct implementation classes based on message type
      var adapterFactory = new AdapterFactory();

      // Create new kdbEnvelope instance from factory for this adapter / config
      envelope =
          adapterFactory.getKdbEnvelope(
              this.getMessageType(), adapterProperties.getKdbEnvelopeSize());

      // Loop until stopped...
      while (!stopped.get()) {

        try (DocumentContext dc = tailer.readingDocument()) {

          if (!dc.isPresent()) {
            // if nothing new in the queue, try sending anything currently in the envelope
            if (!envelope.isEmpty()) {
              try {
                trySend(adapterProperties, envelope);
              } catch (Exception ex) {
                rollbackEnvelope(envelope, tailer);
                log.error("Problem saving envelope {}", ex.getMessage());
                this.stop();
              }
            }
            continue;
          }

          // 3. read message data ( -> chronicle obj)
          // Only read messages of type adapterProperties.getAdapterMessageType() e.g. "QUOTE"

          // Use the right read method from the adapterFactory based on type...
          ChronicleMessage chronicleMessage =
              adapterFactory.readChronicleMessage(this.getMessageType(), dc);

          tailerIndex = tailer.index();

          // 4. Do mapping (chronicle obj -> kdb obj)

          // Get right kind of KdbMessage object for this adapter from factory
          KdbMessage kdbMessage =
              adapterFactory.mapChronicleToKdbMessage(this.getMessageType(), chronicleMessage);

          // 5. Add kdb msg to current kdb envelope

          envelope.addToEnvelope(kdbMessage, tailerIndex, adapterProperties);

          // 6. When envelope / batch full, send data to destination ( -> kdb)

          if (envelope.isFull()) {
            try {
              trySend(adapterProperties, envelope);
            } catch (Exception ex) {
              rollbackEnvelope(envelope, tailer);
              log.error("Problem saving envelope {}", ex.getMessage());
              this.stop();
            }
          }
        }
      }

      // *********
      // If here, stopping thread
      // *********
      processFinish = System.nanoTime() - processStart;
      log.info(
          "Stopping Chronicle kdb Adapter. {} msgs stored in this cycle ({} seconds)",
          howManyStored,
          processFinish / 1e9);

    } catch (Exception ex) {
      log.error("Error in processMessages() -- {}", ex.toString());
    } finally {
      tidyUp();
      stop();
    }
  }

  private void rollbackEnvelope(KdbEnvelope<KdbMessage> envelope, ExcerptTailer tailer) {
    // If error has occured and there are messages in the current envelope
    // rollback tailer on queue so can re-start from correct point
    log.info(FAILED_TO_SAVE);
    // Roll back Chronicle Tailer to (index of 1st msg in envelope - 1)
    log.info("Rolling Chronicle Tailer back to index: {}", envelope.getFirstIndex());
    tailer.moveToIndex(envelope.getFirstIndex());
  }

  private void trySend(AdapterProperties adapterProperties, KdbEnvelope<?> envelope)
      throws AdapterConfigurationException, KdbException {

    long now = System.nanoTime();
    if (now - lastWriteNanos < MIN_SEND_PAUSE_NANOS) return;

    int envelopeDepthBeforeSave = envelope.getEnvelopeDepth();
    saveCurrentEnvelope(adapterProperties, envelope);
    howManyStored += envelopeDepthBeforeSave;
  }

  // Benchmarking version
  private void trySend(AdapterProperties adapterProperties, KdbEnvelope<?> envelope, JLBH jlbh)
      throws AdapterConfigurationException, KdbException {

    long now = System.nanoTime();
    if ((now - lastWriteNanos) < MIN_SEND_PAUSE_NANOS) return;
    // Save current envelope contents...
    saveCurrentEnvelope(adapterProperties, envelope, jlbh);
    lastWriteNanos = System.nanoTime();
  }

  // Benchmarking version
  public void benchmarkProcessMessages() {

    long tailerIndex;
    NanoSampler readMessageSampler = jlbh.addProbe("Adapter readMessage()");
    NanoSampler convertToKDBSampler = jlbh.addProbe("Adapter convertToKDB()");
    writeToKDBSampler = jlbh.addProbe("Adapter writeToKDB() ONLY");

    if (adapterProperties.getCoreAffinity() > -1)
      Affinity.setAffinity(adapterProperties.getCoreAffinity());

    // 1. Connect to Chronicle Queue source
    // 2. Create "tailer" to listen for messages
    try (SingleChronicleQueue sourceQueue =
            SingleChronicleQueueBuilder.binary(adapterProperties.getChronicleSource()).build();
        ExcerptTailer tailer = sourceQueue.createTailer(adapterProperties.getAdapterTailerName())) {

      log.info("Starting Chronicle kdb Adapter");

      // Check last index read / starting index
      tailerIndex = tailer.index();
      log.info("Tailer starting at index: {}", tailerIndex);

      // Use AdapterFactory to return correct implementation classes based on message type
      var adapterFactory = new AdapterFactory();

      // Create new kdbEnvelope instance from factory for this adapter / config
      final KdbEnvelope<KdbMessage> envelope =
          adapterFactory.getKdbEnvelope(
              this.getMessageType(), adapterProperties.getKdbEnvelopeSize());

      // Loop until stopped...
      while (!stopped.get()) {

        try (DocumentContext dc = tailer.readingDocument()) {

          if (!dc.isPresent()) {
            // if nothing new in the queue, try sending anything currently in the envelope
            if (!envelope.isEmpty()) {
              trySend(adapterProperties, envelope, jlbh);
            }
            continue;
          }

          // 3. read message data ( -> chronicle obj)
          // Only read messages of type adapterProperties.getAdapterMessageType() e.g. "QUOTE"

          // Use the right read method from the adapterFactory based on type...
          ChronicleMessage chronicleMessage =
              adapterFactory.readChronicleMessage(this.getMessageType(), dc);

          // Have read a message so capture timestamp and add to probe
          long readSamplerStart = System.nanoTime();
          // TODO Need to make this work on ChronicleMessage rather than casting
          ChronicleQuoteMsg msg = (ChronicleQuoteMsg) chronicleMessage;
          readMessageSampler.sampleNanos(readSamplerStart - msg.getTs());

          // Increment current "read up to index"
          tailerIndex = tailer.index();

          // 4. Do mapping (chronicle obj -> kdb obj)

          // Get right kind of KdbMessage object for this adapter from factory
          KdbMessage kdbMessage =
              adapterFactory.mapChronicleToKdbMessage(this.getMessageType(), chronicleMessage);

          // Converted to Kdb object so capture timestamp and add to probe
          long convertSamplerStart = System.nanoTime();
          KdbQuoteMessage kdbMsg = (KdbQuoteMessage) kdbMessage;
          convertToKDBSampler.sampleNanos(convertSamplerStart - kdbMsg.getTs());

          // 5. Add kdb msg to current kdb envelope

          envelope.addToEnvelope(kdbMessage, tailerIndex, adapterProperties);

          // 6. Every $kdbEnvelopeSize messages, send data to destination ( -> kdb)

          if (envelope.isFull()) {
            // Store
            trySend(adapterProperties, envelope, jlbh);
          }
        }
      }
    } catch (Exception ex) {
      log.error("Error in benchmarkProcessMessages() -- {}", ex.toString());
    } finally {
      tidyUp();
      stop();
    }
  }

  // Benchmarking version
  public void benchmarkKdbOnly() {

    try {
      final int MSG_COUNT = 1_000_000;

      log.info("Starting kdb only Adapter");

      // Use AdapterFactory to return correct implementation classes based on message type
      var adapterFactory = new AdapterFactory();

      // Create new kdbEnvelope instance from factory for this adapter / config
      final KdbEnvelope<KdbMessage> envelope =
          adapterFactory.getKdbEnvelope(
              this.getMessageType(), adapterProperties.getKdbEnvelopeSize());

      var helper = new QuoteHelper();
      ArrayList<KdbQuoteMessage> ar = new ArrayList<>();
      for (var i = 0; i < MSG_COUNT; i++) {
        KdbQuoteMessage kdbMessage = helper.generateKdbQuoteMsg();
        kdbMessage.setTs(System.nanoTime());
        ar.add(kdbMessage);
      }

      int howManyRead = 0;
      log.info("Starting loop");
      long writeSamplerBefore = System.nanoTime();

      // Loop until stopped...
      while (!stopped.get() && howManyRead < MSG_COUNT) {

        KdbQuoteMessage kdbMessage = ar.get(howManyRead);
        // 5. Add kdb msg to current kdb envelope

        envelope.addToEnvelope(kdbMessage, 1L, adapterProperties);

        howManyRead++;

        // 6. Every $kdbEnvelopeSize messages, send data to destination ( -> kdb)

        if (envelope.isFull()) {
          // Store
          trySend(adapterProperties, envelope);
        }
      }
      if (!envelope.isEmpty()) {
        trySend(adapterProperties, envelope);
      }
      log.info("kdb only test took: {} seconds", (System.nanoTime() - writeSamplerBefore) / 1e9);

    } catch (Exception ex) {
      log.error("Error in benchmarkKdbOnly() -- {}", ex.getMessage());
      this.stop();
    } finally {
      tidyUp();
    }
  }

  private void saveCurrentEnvelope(AdapterProperties adapterProperties, KdbEnvelope<?> envelope)
      throws AdapterConfigurationException, KdbException {

    if (kdbConnector == null) {
      kdbConnector = new KdbConnector(adapterProperties);
    }

    kdbConnector.saveEnvelope(adapterProperties, envelope);

    // 7. Envelope contents saved. Re-set envelope...
    envelope.reset();
  }

  private void saveCurrentEnvelope(
      AdapterProperties adapterProperties, KdbEnvelope<?> envelope, JLBH jlbh)
      throws AdapterConfigurationException, KdbException {

    if (kdbConnector == null) {
      kdbConnector = new KdbConnector(adapterProperties);
    }

    // Capture timestamp before writing to kdb
    long writeSamplerBefore = System.nanoTime();

    kdbConnector.saveEnvelope(adapterProperties, envelope);

    // Saved, capture timestamp
    long kdbUpdatedTimeStamp = System.nanoTime();

    // Add sample for batch write on its own
    writeToKDBSampler.sampleNanos(kdbUpdatedTimeStamp - writeSamplerBefore);

    // Add benchmark samples for each message in Envelope here
    // Iterate through envelope and get diff from message creation ts to kdbUpdatedTimeStamp
    // add sample i.e. End to end for each message
    for (long msgCreateTs : envelope.getTs()) {
      jlbh.sample(kdbUpdatedTimeStamp - msgCreateTs);
    }

    // 7. Envelope contents saved. Re-set envelope...
    envelope.reset();
  }
}

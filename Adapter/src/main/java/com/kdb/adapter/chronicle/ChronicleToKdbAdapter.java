package com.kdb.adapter.chronicle;

import com.kdb.adapter.factory.AdapterFactory;
import com.kdb.adapter.messages.*;
import com.kdb.adapter.kdb.KdbConnector;
import com.kdb.adapter.utils.AdapterProperties;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChronicleToKdbAdapter {

  private KdbConnector kdbConnector;
  private static final Logger LOG = LoggerFactory.getLogger(ChronicleToKdbAdapter.class);
  private MessageTypes.AdapterMessageTypes messageType;
  int howManyRead;

  public void setMessageType(MessageTypes.AdapterMessageTypes msgType) {
    this.messageType = msgType;
  }

  public MessageTypes.AdapterMessageTypes getMessageType() {
    return this.messageType;
  }

  public ChronicleToKdbAdapter() {
    // Empty no args constructor
  }

  public ChronicleToKdbAdapter(MessageTypes.AdapterMessageTypes type) {
    this.setMessageType(type);
  }

  public boolean setAdapterMessageType(String messageType) {
    // Set adapter message factory type based on config property
    boolean ret = true;
    if (messageType.equalsIgnoreCase("QUOTE")) {
      this.setMessageType(MessageTypes.AdapterMessageTypes.QUOTE);
    } else if (messageType.equalsIgnoreCase("TRADE")) {
      this.setMessageType(MessageTypes.AdapterMessageTypes.TRADE);
    } else {
      LOG.error("Adapter type ({}) not configured yet. Check config.", messageType);
      ret = false;
    }
    return ret;
  }

  public void tidyUp() {

    try {
      if (kdbConnector != null) {
        kdbConnector.closeConnection();
      }
    } catch (Exception e) {
      LOG.error("Exception can be ignored here..{}", e.toString());
    }
    LOG.debug("Resources cleaned up");
  }

  public int checkMessages(AdapterProperties adapterProperties) {

    try (SingleChronicleQueue queue =
            SingleChronicleQueueBuilder.binary(adapterProperties.getChronicleSource()).build();
        ExcerptTailer tailer = queue.createTailer(adapterProperties.getAdapterTailerName())) {
      LOG.info("Tailer index: {}", tailer.index());
      long count = 0L;
      ChronicleQuoteMsg chronicleMessage;
      long start = System.nanoTime();
      for (; ; ) {
        try (DocumentContext dc = tailer.readingDocument()) {
          if (!dc.isPresent()) break;

          chronicleMessage =
              (ChronicleQuoteMsg)
                  dc.wire().read(adapterProperties.getAdapterMessageType()).object();
          count++;
          LOG.debug("Message: {}", chronicleMessage);
        }
      }
      long finish = System.nanoTime() - start;
      LOG.info("TIMING: Read {} msgs -> ChronicleQuoteMsg obj in {} seconds", count, finish / 1e9);
      LOG.info("No more messages");
    }
    return 0;
  }

  public int processMessages(AdapterProperties adapterProperties) {
    // ret = 0 -- all ok but nothing (left) to process
    // ret = -1 -- problem running

    int ret = 0;
    long tailerIndex;
    long howManyStored = 0L;
    final long processStart = System.nanoTime();
    long processFinish;

    // 1. Connect to Chronicle Queue source
    // 2. Create "tailer" to listen for messages
    try (SingleChronicleQueue sourceQueue =
            SingleChronicleQueueBuilder.binary(adapterProperties.getChronicleSource()).build(); ExcerptTailer tailer =
        sourceQueue.createTailer(adapterProperties.getAdapterTailerName())) {

      LOG.info("Starting Chronicle kdb Adapter");

      // Check last index read / starting index
      tailerIndex = tailer.index();
      LOG.info("Tailer starting at index: {}", tailerIndex);

      // Use AdapterFactory to return correct implementation classes based on message type
      AdapterFactory adapterFactory = new AdapterFactory();

      // Create new kdbEnvelope instance from factory for this adapter / config
      final KdbEnvelope envelope =
          adapterFactory.getKdbEnvelope(
              this.getMessageType(), adapterProperties.getKdbEnvelopeSize());

      // Loop while there are messages...
      for (; ; ) {

        try (DocumentContext dc = tailer.readingDocument()) {

          if (!dc.isPresent()) break;

          // 3. read message data ( -> chronicle obj)
          // Only read messages of type adapterProperties.getAdapterMessageType() e.g. "quote"

          // Use the right read method from the adapterFactory based on type...
          ChronicleMessage chronicleMessage =
              adapterFactory.readChronicleMessage(
                  this.getMessageType(), dc, adapterProperties.getAdapterMessageType());

          tailerIndex = tailer.index();

          howManyRead++;

          // 4. Do mapping (chronicle obj -> kdb obj)

          // Get right kind of KdbMessage object for this adapter from factory
          KdbMessage kdbMessage =
              adapterFactory.mapChronicleToKdbMessage(this.getMessageType(), chronicleMessage);

          // 5. Add kdb msg to current kdb envelope

          envelope.addToEnvelope(kdbMessage, tailerIndex);

          // 6. Every $kdbEnvelopeSize messages, send data to destination ( -> kdb)

          if (envelope.isFull()) {
            if (saveCurrentEnvelope(adapterProperties, envelope, tailer)) {
              howManyStored += adapterProperties.getKdbEnvelopeSize();
            } else {
              // There was a problem saving...
              ret = -1;
            }
          }
        }
      }

      // *********
      // If here, nothing left on queue, check if there are any messages to store...
      if (envelope.getEnvelopeDepth() > 0) {

        // Save current envelope contents...
        int envelopeDepthBeforeSave = envelope.getEnvelopeDepth();
        if (saveCurrentEnvelope(adapterProperties, envelope, tailer)) {
          howManyStored += envelopeDepthBeforeSave;
        } else {
          // There was a problem saving...
          ret = -1;
        }

      } else {
        ret = 0;
      }
      // *********
      processFinish = System.nanoTime() - processStart;
      LOG.info(
          "Stopping Chronicle kdb Adapter. {} msgs stored in this cycle ({} seconds)",
          howManyStored,
          processFinish / 1e9);
      return ret;

    } catch (Exception ex) {
      LOG.error("Error in processMessages() -- {}", ex.toString());
      return -1;
    }
  }

  private boolean saveCurrentEnvelope(
      AdapterProperties adapterProperties, KdbEnvelope envelope, ExcerptTailer tailer) {

    boolean retVal = true;

    if (kdbConnector == null) {
      kdbConnector = new KdbConnector(adapterProperties);
    }

    if (kdbConnector.saveEnvelope(adapterProperties, envelope)) {

      // 7. Envelope contents saved. Re-set envelope...
      envelope.reset();
      howManyRead = 0;
    } else {
      LOG.info("Failed to save current envelope.");
      // Roll back Chronicle Tailer to (index of 1st msg in envelope - 1)
      LOG.info("Rolling Chronicle Tailer back to index: {}", envelope.getFirstIndex());
      tailer.moveToIndex(envelope.getFirstIndex());
      retVal = false;
    }

    return retVal;
  }
}

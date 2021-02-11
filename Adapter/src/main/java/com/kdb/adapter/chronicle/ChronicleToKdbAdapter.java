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
  private long start;
  private long finish;
  private static final Logger LOG = LoggerFactory.getLogger(ChronicleToKdbAdapter.class);
  private MessageTypes.AdapterMessageTypes messageType;
  long tailerIndex;
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
      start = System.nanoTime();
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
      finish = System.nanoTime() - start;
      LOG.info("TIMING: Read {} msgs -> ChronicleQuoteMsg obj in {} seconds", count, finish / 1e9);
      LOG.info("No more messages");
    }
    return 0;
  }

  public int processMessages(AdapterProperties adapterProperties) {
    // ret = 0 -- all ok but nothing (left) to process
    // ret = -1 -- problem running

    int ret = 0;
    long howManyStored = 0L;
    final long processStart = System.nanoTime();
    long processFinish;

    // 1. Connect to Chronicle Queue source
    // 2. Create "tailer" to listen for messages
    try (SingleChronicleQueue queue =
            SingleChronicleQueueBuilder.binary(adapterProperties.getChronicleSource()).build();
        ExcerptTailer tailer = queue.createTailer(adapterProperties.getAdapterTailerName())) {

      LOG.info("Starting Chronicle kdb Adapter");

      // Check last index read / starting index
      tailerIndex = tailer.index();
      LOG.info("Tailer starting at index: {}", tailerIndex);

      // Use AdapterFactory to return correct implementation classes based on message type
      AdapterFactory adapterFactory = new AdapterFactory();

      // Create new kdbEnvelope instance from factory for this adapter / config
      final KdbEnvelope envelope = adapterFactory.getKdbEnvelope(this.getMessageType());

      // Loop while there are messages...
      for (; ; ) {

        try (DocumentContext dc = tailer.readingDocument()) {

          if (!dc.isPresent()) break;

          // 3. read message data ( -> chronicle obj)
          // Only read messages of type adapterProperties.getAdapterMessageType() e.g. "quote"

          start = System.nanoTime();

          // Use the right read method from the adapterFactory based on type...
          ChronicleMessage chronicleMessage =
              adapterFactory.readChronicleMessage(
                  this.getMessageType(), dc, adapterProperties.getAdapterMessageType());

          finish = System.nanoTime() - start;
          LOG.trace("TIMING: msg -> ChronMsg obj in {} seconds", finish / 1e9);

          tailerIndex = tailer.index();
          LOG.debug("Read message {} @ index: {}", chronicleMessage, tailerIndex);

          howManyRead++;

          // 4. Do mapping (chronicle obj -> kdb obj)

          start = System.nanoTime();

          // Get right kind of KdbMessage object for this adapter from factory
          KdbMessage kdbMessage =
              adapterFactory.mapChronicleToKdbMessage(this.getMessageType(), chronicleMessage);

          finish = System.nanoTime() - start;
          LOG.trace("TIMING: mapped msg -> kdbMsg obj in {} seconds", finish / 1e9);

          // 5. Add kdb msg to current kdb envelope

          start = System.nanoTime();

          envelope.addToEnvelope(kdbMessage, tailerIndex);

          finish = System.nanoTime() - start;
          LOG.debug("TIMING: Added msg -> envelope obj in {} seconds", finish / 1e9);

          // 6. Every $kdbEnvelopeSize messages, send data to destination ( -> kdb)

          if (howManyRead == adapterProperties.getKdbEnvelopeSize()) {

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

        LOG.debug(
            "Nothing else to process; current envelope size = {}", envelope.getEnvelopeDepth());

        // Save current envelope contents...
        int envelopeDepthBeforeSave = envelope.getEnvelopeDepth();
        if (saveCurrentEnvelope(adapterProperties, envelope, tailer)) {
          howManyStored += envelopeDepthBeforeSave;
        } else {
          // There was a problem saving...
          ret = -1;
        }

      } else {
        LOG.debug("Nothing (left) to process and nothing to save");
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

    start = System.nanoTime();

    if (kdbConnector == null) {
      kdbConnector = new KdbConnector(adapterProperties);
    }

    if (kdbConnector.saveEnvelope(adapterProperties, envelope)) {

      finish = System.nanoTime() - start;
      LOG.info(
          "TIMING: Stored {} messages (up to index: {}) in {} seconds",
          howManyRead,
          tailerIndex,
          finish / 1e9);

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

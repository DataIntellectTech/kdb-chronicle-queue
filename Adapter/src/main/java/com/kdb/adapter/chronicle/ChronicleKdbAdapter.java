package com.kdb.adapter.chronicle;

import com.kdb.adapter.mapper.SourceToDestinationMapper;
import com.kdb.adapter.messages.ChronicleQuoteMsg;
import com.kdb.adapter.kdb.KdbConnector;
import com.kdb.adapter.messages.KdbEnvelope;
import com.kdb.adapter.messages.KdbMessage;
import com.kdb.adapter.messages.KdbQuoteMessage;
import com.kdb.adapter.timer.AdapterTimer;
import com.kdb.adapter.utils.AdapterProperties;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.mapstruct.factory.Mappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Future;

public class ChronicleKdbAdapter {

  private KdbConnector kdbConnector;
  private boolean keepRunning = true;
  private long tailerIndex;
  private int howManyRead;
  private long howManyStored;
  private AdapterTimer adapterTimer;
  private Future<Boolean> tooLongSinceLastMsg;
  private long start;
  private long finish;
  private SingleChronicleQueue queue;
  private ExcerptTailer tailer;
  private static Logger LOG = LoggerFactory.getLogger(ChronicleKdbAdapter.class);
  private SourceToDestinationMapper mapper = Mappers.getMapper(SourceToDestinationMapper.class);
  private ChronicleQuoteMsg quote;

  public void tidyUp() {

    keepRunning = false;

    try {

      if (kdbConnector != null) {
        kdbConnector.closeConnection();
      }

      if (tailer != null) {
        tailer.close();
      }
      if (queue != null) {
        queue.close();
      }
    } catch (Exception e) {
    } // Can ignore anything at this stage...

    LOG.debug("Resources cleaned up");
  }

  public int processMessages(AdapterProperties adapterProperties) {
    // ret = 0 -- all ok but nothing (left) to process
    // ret = -1 -- problem running

    int ret = 0;
    keepRunning = true;
    howManyStored = 0L;
    final long processStart = System.nanoTime();
    long processFinish;

    try {

      LOG.info("Starting Chronicle kdb Adapter");

      // 1. Connect to Chronicle Queue source

      queue = SingleChronicleQueueBuilder.binary(adapterProperties.getChronicleSource()).build();

      // 2. Create "tailer" to listen for messages

      tailer = queue.createTailer(adapterProperties.getAdapterTailerName());

      tailerIndex = tailer.index();
      LOG.info("Tailer starting at index: {}", tailerIndex);

      // Create new kdbEnvelope instance
      final KdbEnvelope envelope = new KdbEnvelope();

      adapterTimer = new AdapterTimer();

      tooLongSinceLastMsg =
          adapterTimer.tooLongSinceLastMsg(adapterProperties.getKdbEnvelopeWaitTime());

      while (keepRunning) {

        // Do some timer checks...

        if (tooLongSinceLastMsg.isDone()) {

          final int currEnvelopeDepth = envelope.getEnvelopeDepth();

          if (currEnvelopeDepth > 0) {

            LOG.debug(
                "Waited too long with msgs to go; envelope size = {}", envelope.getEnvelopeDepth());

            if (saveCurrentEnvelope(adapterProperties, envelope)) {
              howManyStored += currEnvelopeDepth;
              keepRunning = true;
            } else {
              ret = -1;
              keepRunning = false;
              break;
            }

          } else {
            LOG.debug("Nothing (left) to process");
            ret = 0;
            keepRunning = false;
            break;
          }
        }

        // Start "normal" queue processing work...

        // Only read messages of type messageType e.g. "quote"
        tailer.readDocument(
            q ->
                q.read(adapterProperties.getAdapterMessageType())
                    .marshallable(
                        m -> {

                          // 3. read message data ( -> chronicle obj)

                          start = System.nanoTime();

                          quote =
                              new ChronicleQuoteMsg.ChronicleQuoteMsgBuilder()
                                  .withTime(m.read("time").dateTime())
                                  .withSym(m.read("sym").text())
                                  .withBid(m.read("bid").float64())
                                  .withBsize(m.read("bsize").float64())
                                  .withAsk(m.read("ask").float64())
                                  .withAssize(m.read("assize").float64())
                                  .withBex(m.read("bex").text())
                                  .withAex(m.read("aex").text())
                                  .build();

                          finish = System.nanoTime() - start;
                          LOG.trace("TIMING: msg -> ChronMsg obj in {} seconds", finish / 1e9);

                          tailerIndex = tailer.index();
                          LOG.debug("Read message @ index: {}", tailerIndex);

                          howManyRead++;

                          // 4. Do mapping (chronicle obj -> kdb obj)

                          start = System.nanoTime();

                          KdbQuoteMessage kdbQuoteMsg = mapper.sourceToDestination(quote);

                          finish = System.nanoTime() - start;
                          LOG.trace("TIMING: mapped msg -> kdbMsg obj in {} seconds", finish / 1e9);

                          quote = null;

                          // 5. Add kdb msg to current kdb envelope

                          start = System.nanoTime();

                          envelope.addToEnvelope(kdbQuoteMsg, tailerIndex);

                          finish = System.nanoTime() - start;
                          LOG.trace(
                              "TIMING: Added msg -> envelope obj in {} seconds", finish / 1e9);

                          kdbQuoteMsg = null;

                          // Cancel & Start new timer ------------------------

                          tooLongSinceLastMsg.cancel(false);
                          tooLongSinceLastMsg =
                              adapterTimer.tooLongSinceLastMsg(
                                  adapterProperties.getKdbEnvelopeWaitTime());

                          // 6. Every $kdbEnvelopeSize messages, send data to destination ( -> kdb)

                          if (howManyRead == adapterProperties.getKdbEnvelopeSize()) {

                            if (saveCurrentEnvelope(adapterProperties, envelope)) {
                              howManyStored += adapterProperties.getKdbEnvelopeSize();
                              keepRunning = true;
                            } else {
                              keepRunning = false;
                            }
                          }
                        }));
      }
      processFinish = System.nanoTime() - processStart;
      LOG.info(
          "Stopping Chronicle kdb Adapter. {} msgs stored in this cycle ({} seconds)",
          howManyStored,
          processFinish / 1e9);
      return ret;

    } catch (Exception ex) {
      LOG.error("Error in processMessages() -- {}", ex.toString());
      return -1;
    } finally {
      try {
        if (tailer != null) {
          tailer.close();
        }
        if (queue != null) {
          queue.close();
        }
      } catch (Exception e) {
      }
    }
  }

  private boolean saveCurrentEnvelope(AdapterProperties adapterProperties, KdbEnvelope envelope) {

    boolean retVal = true;

    start = System.nanoTime();

    if (kdbConnector == null) {
      kdbConnector = new KdbConnector(adapterProperties);
    }

    if (kdbConnector.saveMessage(adapterProperties, envelope)) {

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

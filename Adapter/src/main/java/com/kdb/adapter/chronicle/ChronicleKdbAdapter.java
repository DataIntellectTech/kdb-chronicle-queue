package com.kdb.adapter.chronicle;

import com.kdb.adapter.mapper.SourceToDestinationMapper;
import com.kdb.adapter.messages.ChronicleQuoteMsg;
import com.kdb.adapter.kdb.KdbConnector;
import com.kdb.adapter.messages.KdbEnvelope;
import com.kdb.adapter.messages.KdbMessage;
import com.kdb.adapter.timer.AdapterTimer;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.mapstruct.factory.Mappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;
import java.util.concurrent.Future;

@Component
@Endpoint(id = "status")
public class ChronicleKdbAdapter {

    @Autowired
    KdbConnector kdbConnector;

    @Value("${chronicle.source}")
    private String chronicleQueueSource;

    @Value("${adapter.tailerName}")
    private String tailerName;

    @Value("${adapter.messageType}")
    private String messageType;

    @Value("${kdb.destination}")
    private String kdbDestination;

    @Value("${kdb.destination.function}")
    private String kdbDestinationFunction;

    @Value("${kdb.envelope.size: 20}")
    private int kdbEnvelopeSize;

    @Value("${kdb.envelope.waitTime: 1000}")
    private long kdbEnvelopeWaitTime;

    private boolean keepRunning = true;
    private long tailerIndex = 0L;
    private int howManyRead = 0;
    private long howManyStored = 0L;
    AdapterTimer adapterTimer;
    Future<Boolean> tooLongSinceLastMsg;
    private long start;
    private long finish;
    SingleChronicleQueue queue = null;
    ExcerptTailer tailer = null;
    private static Logger LOG = LoggerFactory.getLogger(ChronicleKdbAdapter.class);
    private SourceToDestinationMapper mapper = Mappers.getMapper(SourceToDestinationMapper.class);
    private ChronicleQuoteMsg quote;

    @ReadOperation
    public Message read() {
        return new Message(String.format("Adapter info: Tailer [%s] processing queue [%s] writing to KDB [%s] Last message index [%s]", tailerName, chronicleQueueSource, kdbDestination, tailerIndex));
    }

    @Getter
    @RequiredArgsConstructor
    public static class Message {
        private final String message;
    }

    public void tidyUp() {

        keepRunning = false;

        try {

            //TO DO kdb tidy up?
            if (kdbConnector != null) {
                kdbConnector.closeConnection();
            }

            //TO DO Chronicle tidy up?
            if (tailer != null) {
                //tailer.readingDocument().close();
                tailer.close();
            }
            if (queue != null) {
                queue.close();
            }
        }
        catch(Exception e){} //Can ignore anything at this stage...

        LOG.info("Resources cleaned up");
    }

    public int processMessages() {
        // ret = 0 -- all ok but nothing (left) to process
        // ret = -1 -- problem running

        int ret = 0;
        keepRunning=true;
        howManyStored=0L;

        try {

            LOG.info("Starting Chronicle kdb Adapter");

            // 1. Connect to Chronicle Queue source

            queue = SingleChronicleQueueBuilder.binary(chronicleQueueSource).build();

            // 2. Create "tailer" to listen for messages

            tailer = queue.createTailer(tailerName);

            tailerIndex = tailer.index();
            LOG.info("Tailer starting at index: " + tailerIndex);

            // Create new kdbEnvelope instance
            KdbEnvelope envelope = new KdbEnvelope();

            adapterTimer = new AdapterTimer();

            tooLongSinceLastMsg = adapterTimer.tooLongSinceLastMsg(kdbEnvelopeWaitTime);

            while (keepRunning) {

                // Do some timer checks...

                if (tooLongSinceLastMsg.isDone()) {

                    int currEnvelopeDepth=envelope.getEnvelopeDepth();

                    if (currEnvelopeDepth > 0) {

                        LOG.debug("Waited too long with msgs to go; envelope size = " + envelope.getEnvelopeDepth());

                        if (saveCurrentEnvelope(envelope)){
                            howManyStored+=currEnvelopeDepth;
                            keepRunning=true;
                        }
                        else{
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
                tailer.readDocument(q -> q.read(messageType)
                        .marshallable(
                                m -> {

                                    // 3. read message data ( -> chronicle obj)

                                    start = System.nanoTime();

                                    quote = new ChronicleQuoteMsg
                                            .ChronicleQuoteMsgBuilder()
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
                                    LOG.trace("TIMING: msg -> ChronMsg obj in " + finish / 1e9 + " seconds");

                                    tailerIndex = tailer.index();
                                    LOG.debug("Read message @ index: " + tailerIndex);

                                    howManyRead++;

                                    // 4. Do mapping (chronicle obj -> kdb obj)

                                    start = System.nanoTime();

                                    KdbMessage kdbMsg = mapper.sourceToDestination(quote);

                                    finish = System.nanoTime() - start;
                                    LOG.trace("TIMING: mapped msg -> kdbMsg obj in " + finish / 1e9 + " seconds");

                                    quote = null;

                                    // 5. Add kdb msg to current kdb envelope

                                    start = System.nanoTime();

                                    envelope.addToEnvelope(kdbMsg, tailerIndex);

                                    finish = System.nanoTime() - start;
                                    LOG.trace("TIMING: Added msg -> envelope obj in " + finish / 1e9 + " seconds");

                                    kdbMsg = null;

                                    // Start new timer ------------------------

                                    tooLongSinceLastMsg.cancel(false); // = null;
                                    tooLongSinceLastMsg = adapterTimer.tooLongSinceLastMsg(kdbEnvelopeWaitTime);

                                    // 6. Every $kdbEnvelopeSize messages, send data to destination ( -> kdb)

                                    if (howManyRead == kdbEnvelopeSize) {

                                        if (saveCurrentEnvelope(envelope)){
                                            howManyStored+=howManyRead;
                                            keepRunning=true;
                                        }
                                        else{
                                            keepRunning=false;
                                        }

                                    }
                                }
                        )
                );
            }
            LOG.info("Stopping Chronicle kdb Adapter. " + howManyStored + " msgs stored in this cycle.");
            return ret;

        } catch (Exception ex) {
            LOG.error("Error in processMessages() -- " + ex.toString());
            return -1;
        } finally {
            try {
                if (tailer != null) {
                    //tailer.readingDocument().close();
                    tailer.close();
                }
                if (queue != null) {
                    queue.close();
                }
            }
            catch(Exception e){}
        }
    }

    private boolean saveCurrentEnvelope(KdbEnvelope envelope){

        boolean retVal=true;

        start = System.nanoTime();

        if (kdbConnector.saveMessage(envelope, kdbDestination, kdbDestinationFunction)) {

            finish = System.nanoTime() - start;
            LOG.trace("TIMING: Stored " + howManyRead + " messages (up to index: " + tailerIndex + ") in " + finish / 1e9 + " seconds");

            // 7. Envelope contents saved. Re-set envelope...
            envelope.reset();
            howManyRead = 0;
        } else {
            LOG.info("Failed to save current envelope.");
            // Roll back Chronicle Tailer to (index of 1st msg in envelope - 1)
            LOG.info("Rolling Chronicle Tailer back to index: " + envelope.getFirstIndex());
            tailer.moveToIndex(envelope.getFirstIndex());
            retVal=false;
        }

        return retVal;
    }

}

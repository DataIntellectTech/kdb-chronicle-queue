package com.kdb.adapter.chronicle;

import com.kdb.adapter.mapper.SourceToDestinationMapper;
import com.kdb.adapter.messages.ChronicleQuoteMsg;
import com.kdb.adapter.kdb.KdbConnector;
import com.kdb.adapter.messages.ChronicleQuoteMsgBuilder;
import com.kdb.adapter.messages.KdbEnvelope;
import com.kdb.adapter.messages.KdbMessage;
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

import java.time.LocalDateTime;
import java.util.Timer;

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

    @Value("${kdb.envelope.waitTime: 100}")
    private int kdbEnvelopeWaitTime;

    private boolean keepRunning=true;
    private long lastIndex = 0L;
    private int howManyRead=0;
    private Timer timer;

    private static Logger LOG = LoggerFactory.getLogger(ChronicleKdbAdapter.class);

    private SourceToDestinationMapper mapper = Mappers.getMapper(SourceToDestinationMapper.class);

    @ReadOperation
    public Message read() {
        return new Message(String.format("Adapter info: Tailer [%s] processing queue [%s] writing to KDB [%s] Last message index [%s]", tailerName , chronicleQueueSource, kdbDestination, lastIndex));
    }

    @Getter
    @RequiredArgsConstructor
    public static class Message {
        private final String message;
    }

    public void tidyUp(){

        keepRunning=false;

        //TO DO kdb tidy up?
        //TO DO Chronicle tidy up?

        LOG.info("Resources cleaned up");
    }

    public void processMessages(){

        LOG.info("Starting Chronicle kdb Adapter");

        // 1. Connect to Chronicle Queue source

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(chronicleQueueSource).build();

        // 2. Create "tailer" to listen for messages

        ExcerptTailer tailer = queue.createTailer(tailerName);

        this.lastIndex = tailer.index();
        LOG.info("Tailer starting at index: " + lastIndex);

        // Create new kdbEnvelope instance
        KdbEnvelope envelope = new KdbEnvelope();

        while (keepRunning) {

            // Only read messages of type messageType
            tailer.readDocument(q -> q.read(messageType)
                    .marshallable(
                            m -> {

                                // 3. read message data ( -> chronicle obj)

                                ChronicleQuoteMsg quote = new ChronicleQuoteMsgBuilder()
                                        .setTime(m.read("time").dateTime())
                                        .setSym(m.read("sym").text())
                                        .setBid(m.read("bid").float64())
                                        .setBsize(m.read("bsize").float64())
                                        .setAsk(m.read("ask").float64())
                                        .setAssize(m.read("assize").float64())
                                        .setBex(m.read("bex").text())
                                        .setAex(m.read("aex").text())
                                        .build();

                                howManyRead+=1;

                                // 4. Do mapping (chronicle obj -> kdb obj)

                                KdbMessage kdbMsg = mapper.sourceToDestination(quote);

                                quote = null;

                                // 5. Add kdb msg to current kdb envelope

                                envelope.addToEnvelope(kdbMsg);

                                kdbMsg = null;

                                this.lastIndex = tailer.index();
                                LOG.debug("Read message @ index: " + lastIndex);

                                // 6. Every $kdbEnvelopeSize messages, send data to destination ( -> kdb)

                                if (howManyRead == kdbEnvelopeSize){

                                    kdbConnector.saveMessage(envelope, kdbDestination, kdbDestinationFunction);

                                    // 7. Re-set envelope

                                    envelope.reset();
                                    howManyRead=0;
                                }

                            }
                            )
            );
        }

        LOG.info("Stopping Chronicle kdb Adapter");

        tailer.readingDocument().close();
        queue.close();

        kdbConnector.closeConnection();
    }
}

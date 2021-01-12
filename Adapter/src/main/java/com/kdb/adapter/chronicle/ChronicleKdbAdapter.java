package com.kdb.adapter.chronicle;

import com.kdb.adapter.mapper.SourceToDestinationMapper;
import com.kdb.adapter.messages.ChronicleQuoteMsg;
import com.kdb.adapter.kdb.KdbConnector;
import com.kdb.adapter.messages.ChronicleQuoteMsgBuilder;
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

@Component
@Endpoint(id = "status")
public class ChronicleKdbAdapter {

    @Autowired
    KdbConnector kdbConnector;

    @Value("${adapter.tailerName}")
    String tailerName;

    @Value("${kdb.destination}")
    private String kdbDestination;

    @Value("${kdb.destination.function}")
    private String kdbDestinationFunction;

    private String chronicleQueueSource="";
    boolean keepRunning=true;
    long lastIndex = 0L;

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


    public void tidyUp(String chronicleQueueSource){

        keepRunning=false;

        //kdb tidy up?
        //Chronicle tidy up?

        LOG.info("Resources cleaned up");
    }

    public void processMessages(String chronicleQueueSource){

        LOG.info("Starting Chronicle kdb Adapter");

        this.chronicleQueueSource = chronicleQueueSource;

        // 1. Connect to Chronicle Queue source

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(chronicleQueueSource).build();

        // 2. Create "tailer" to listen for messages

        ExcerptTailer tailer = queue.createTailer(tailerName);

        this.lastIndex = tailer.index();
        LOG.info("Tailer starting at index: " + lastIndex);

        while (keepRunning) {

            tailer.readDocument(q -> q.read("quote")
                    .marshallable(
                            m -> {

                                // 3. read message data ( -> chronicle obj)

                                ChronicleQuoteMsg quote = new ChronicleQuoteMsgBuilder()
                                        .setTime(m.read("time").text())
                                        .setSym(m.read("sym").text())
                                        .setBid(m.read("bid").text())
                                        .setBsize(m.read("bsize").text())
                                        .setAsk(m.read("ask").text())
                                        .setAssize(m.read("assize").text())
                                        .setBex(m.read("bex").text())
                                        .setAex(m.read("aex").text())
                                        .build();

                                // 4. Do mapping (chronicle obj -> kdb obj)

                                KdbMessage kdbMsg = mapper.sourceToDestination(quote);

                                // 5. Send data to destination ( -> kdb)

                                kdbConnector.saveMessage(kdbMsg, kdbDestination, kdbDestinationFunction);

                                this.lastIndex = tailer.index();
                                LOG.info("Processed message @ index: " + lastIndex);
                            }
                            )
            );
        }

        tailer.readingDocument().close();
        queue.close();

        LOG.info("Stopping Chronicle kdb Adapter");

    }
}

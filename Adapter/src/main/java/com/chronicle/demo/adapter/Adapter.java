package com.chronicle.demo.adapter;

import com.chronicle.demo.adapter.messages.ChronicleQuoteMsg;
import com.chronicle.demo.adapter.kdb.KdbConnector;
import com.chronicle.demo.adapter.messages.KdbMessage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

@Component
@Endpoint(id = "adapterinfo")
public class Adapter{

    @Autowired
    KdbConnector kdbConnector;

    @Value("${adapter.tailerName}")
    String tailerName;

    @Value("${kdb.destination}")
    private String kdbDestination;

    private String chronicleQueueSource="";
    boolean keepRunning=true;
    long lastIndex = 0L;

    private static Logger LOG = LoggerFactory.getLogger(Adapter.class);

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

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(chronicleQueueSource).build();
        ExcerptTailer tailer = queue.createTailer(tailerName);

        this.lastIndex = tailer.index();
        LOG.info("Tailer starting at index: " + lastIndex);

        while (keepRunning) {

            tailer.readDocument(q -> q.read("quote")
                    .marshallable(
                            m -> {
                                String time = m.read("time").text();
                                String sym = m.read("sym").text();
                                String bid = m.read("bid").text();
                                String bsize = m.read("bsize").text();
                                String ask = m.read("ask").text();
                                String assize = m.read("assize").text();
                                String bex = m.read("bex").text();
                                String aex = m.read("aex").text();

                                ChronicleQuoteMsg quote = new ChronicleQuoteMsg(time,sym,bid,bsize,ask,assize,bex,aex);
                                KdbMessage kdbMsg = new KdbMessage(quote.toString(), kdbDestination, ".u.upd");
                                kdbConnector.saveMessage(kdbMsg);
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

package com.chronicle.demo.producer.controllers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BinaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private static Logger LOG = LoggerFactory.getLogger(ProducerController.class);

    @Value("${chronicle.quote.queue}")
    String quoteQueuePath;

    @Value("${producer.messageFrequency: 1000}")
    int messageFrequency;

    @Value("${producer.messageLimit}")
    int producerMessageLimit;

    private boolean startQuoteGenerator = false;

    @GetMapping(value = "/quoteLoader")
    public String quoteLoader(@RequestParam(value = "Command: start/stop", required=true)  String command) {
        try {
            if ("START".equals(command.toUpperCase())){
                startQuoteGenerator = true;
                quoteGenerator();
            } else if ("STOP".equals(command.toUpperCase())){
                startQuoteGenerator = false;
            }
        } catch (Exception e) {
            return "*** Encountered error in method quoteLoader: " + e.getMessage();
        }
        LOG.info("*** Successfully executed query");
        return ("*** Successfully executed query");
    }

    public void quoteGenerator() throws InterruptedException {

        long numMsgsWritten=0L;

        List<List<String>> symbolsAndExchanges = new ArrayList<>();
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("VOD.L", "150", "156", "XLON"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("HEIN.AS", "100", "105", "XAMS"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("JUVE.MI", "1230", "1240", "XMIC"));

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(quoteQueuePath).build();;
        ExcerptAppender appender = queue.acquireAppender();

        long start = System.nanoTime();

        while (startQuoteGenerator && (producerMessageLimit == 0 || numMsgsWritten < producerMessageLimit) ) {
            // Only pause if config > 0
            if(messageFrequency>0){
                Thread.sleep(messageFrequency);
            }
            List<String> entry = symbolsAndExchanges.get(new Random().nextInt(symbolsAndExchanges.size()));
            int randomBidPrice = getRandomIntFromRange.apply(Integer.parseInt(entry.get(1)),Integer.parseInt(entry.get(2)));
            int randomAskPrice = getRandomIntFromRange.apply(Integer.parseInt(entry.get(1)),Integer.parseInt(entry.get(2)));

            // Quote data fields...
            // time : 2020.01.24+14:00:16.083Z
            // sym : VOD.L
            // bid : 152
            // bsize : 42035
            // ask : 152
            // assize : 48514
            // bex : XLON
            // aex : XLON

            appender.writeDocument(w -> w.write("quote").marshallable(
                    m -> m.write("time").dateTime(LocalDateTime.now())
                            .write("sym").text(entry.get(0))
                            .write("bid").float64(randomBidPrice)
                            .write("bsize").float64(getRandomIntFromRange.apply(1000,50000))
                            .write("ask").float64(randomAskPrice)
                            .write("assize").float64(getRandomIntFromRange.apply(1000,50000))
                            .write("bex").text(entry.get(3))
                            .write("aex").text(entry.get(3))
            ));

            long index = appender.lastIndexAppended();
            numMsgsWritten++;
            LOG.debug("*** Quote Message written to index ["+ index +"] / (" + numMsgsWritten + " written)");
        }

        long finish = System.nanoTime() - start;
        LOG.info("TIMING: Added "+ producerMessageLimit + " messages (up to index: " + appender.lastIndexAppended() + ") in " + finish / 1e9 + " seconds");

        queue.close();

    }

    private static List<String> buildListOfSymbolExchangeAndPrice(String symbol, String low, String high, String exchange) {
        List<String> items = new ArrayList<>();
        items.add(symbol);
        items.add(low);
        items.add(high);
        items.add(exchange);

        return items;
    }

    BinaryOperator<Integer> getRandomIntFromRange = (x, y) -> {
        Random random = new Random();
        return random.nextInt(y - x) + x;
    };

}

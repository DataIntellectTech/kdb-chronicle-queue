package com.chronicle.demo.producer.controllers;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    @Value("${chronicle.quote.queue}")
    String quoteQueuePath;

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
        List<String> tradeMsgFields = new ArrayList<>();
        tradeMsgFields.add("time");
        tradeMsgFields.add("sym");
        tradeMsgFields.add("bid");
        tradeMsgFields.add("bsize");
        tradeMsgFields.add("ask");
        tradeMsgFields.add("assize");
        tradeMsgFields.add("bex");
        tradeMsgFields.add("aex");

        List<List<String>> symbolsAndExchanges = new ArrayList<>();
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("VOD.L", "150", "156", "XLON"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("HEIN.AS", "100", "105", "XAMS"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("JUVE.MI", "1230", "1240", "XMIC"));

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(quoteQueuePath).build();;
        ExcerptAppender appender = queue.acquireAppender();

        long numMsgsWritten=0L;

        while (startQuoteGenerator) {
            Thread.sleep(2000);
            List<String> entry = symbolsAndExchanges.get(new Random().nextInt(symbolsAndExchanges.size()));
            int randomBidPrice = getRandomNumberBetweenTwoNumbers(entry.get(1),entry.get(2));

            // Quote data fields...
            // time : 2020.01.24+14:00:16.083Z
            // sym : VOD.L
            // bid : 152
            // bsize : 42035
            // ask : 152
            // assize : 48514
            // bex : XLON
            // aex : XLON

            //Build random message
            String msg = buildMsg(tradeMsgFields, entry, randomBidPrice, formatTimeForKdb());
            String[] msgSplit = msg.split((","));

            // value(`upd;`quote;(2020.01.24+14:20:29.217Z;`VOD.L;150;40918;150;11964;`XLON;`XLON))
            //kafkaTemplate.send("quote.t", "quoteKey", String.format("value(`upd;`quote;(%s;`%s;%s;%s;%s;%s;`%s;`%s))", msgSplit[0].substring(7).trim(), msgSplit[1].split(":")[1].trim(), msgSplit[2].split(":")[1].trim(), msgSplit[3].split(":")[1].trim(), msgSplit[4].split(":")[1].trim(), msgSplit[5].split(":")[1].trim(), msgSplit[6].split(":")[1].trim(), msgSplit[7].split(":")[1].trim()));

            appender.writeDocument(w -> w.write("quote").marshallable(
                m -> m.write("time").text(msgSplit[0].substring(7).trim())
                        .write("sym").text(msgSplit[1].split(":")[1].trim())
                        .write("bid").text(msgSplit[2].split(":")[1].trim())
                        .write("bsize").text(msgSplit[3].split(":")[1].trim())
                        .write("ask").text(msgSplit[4].split(":")[1].trim())
                        .write("assize").text(msgSplit[5].split(":")[1].trim())
                        .write("bex").text(msgSplit[6].split(":")[1].trim())
                        .write("aex").text(msgSplit[7].split(":")[1].trim())
            ));

            long index = appender.lastIndexAppended();
            numMsgsWritten++;
            //log me
            LOG.info("*** Quote Message written to index ["+ index +"] / (" + numMsgsWritten + " written) / Content: "+ buildMsg(tradeMsgFields, entry, randomBidPrice, Calendar.getInstance().toInstant().toString()));
        }

        queue.close();

    }

    private String formatTimeForKdb() {
        String time = Calendar.getInstance().toInstant().toString();
        time = time.replace("-",".");
        time = time.replace("T","+");
        return time;
    }

    private String buildMsg(List<String> tradeMsgFields, List<String> entry, int randomBidPrice, String time) {
        StringBuilder tradeMsg = new StringBuilder();
        tradeMsg.append(tradeMsgFields.get(0) + " : " + time + ", ");
        tradeMsg.append(tradeMsgFields.get(1) + " : " + entry.get(0) + ", ");
        tradeMsg.append(tradeMsgFields.get(2) + " : " + randomBidPrice + ", ");
        tradeMsg.append(tradeMsgFields.get(3) + " : " + getRandomNumberBetweenTwoNumbers("1000","50000") + ", ");
        tradeMsg.append(tradeMsgFields.get(4) + " : " + randomBidPrice++ + ", ");
        tradeMsg.append(tradeMsgFields.get(5) + " : " + getRandomNumberBetweenTwoNumbers("1000","50000") + ", ");
        tradeMsg.append(tradeMsgFields.get(6) + " : " + entry.get(3) + ", ");
        tradeMsg.append(tradeMsgFields.get(7) + " : " + entry.get(3));

        return tradeMsg.toString();
    }

    private static List<String> buildListOfSymbolExchangeAndPrice(String symbol, String low, String high, String exchange) {
        List<String> items = new ArrayList<>();
        items.add(symbol);
        items.add(low);
        items.add(high);
        items.add(exchange);

        return items;
    }

    private static int getRandomNumberBetweenTwoNumbers(String low, String high) {
        Random r = new Random();

        return r.nextInt(Integer.parseInt(high)-Integer.parseInt(low)) + Integer.parseInt(low);
    }

}

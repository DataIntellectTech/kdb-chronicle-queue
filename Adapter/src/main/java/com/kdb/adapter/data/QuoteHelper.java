package com.kdb.adapter.data;

import com.kdb.adapter.messages.ChronicleQuoteMsg;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BinaryOperator;

public class QuoteHelper {

    // Quote data fields and examples...
    // time : 2020.01.24+14:00:16.083Z
    // sym : VOD.L
    // bid : 152
    // bsize : 42035
    // ask : 152
    // assize : 48514
    // bex : XLON
    // aex : XLON

    List<List<String>> symbolsAndExchanges;
    List<String> entry;

    public QuoteHelper() {
        symbolsAndExchanges = new ArrayList<>();
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("VOD.L", "150", "156", "XLON"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("HEIN.AS", "100", "105", "XAMS"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("JUVE.MI", "1230", "1240", "XMIC"));
        entry = symbolsAndExchanges.get(new Random().nextInt(symbolsAndExchanges.size()));
    }

    public ChronicleQuoteMsg generateQuoteMsg() {

        int randomBidPrice =
                getRandomIntFromRange.apply(Integer.parseInt(entry.get(1)), Integer.parseInt(entry.get(2)));
        int randomAskPrice =
                getRandomIntFromRange.apply(Integer.parseInt(entry.get(1)), Integer.parseInt(entry.get(2)));

        return new ChronicleQuoteMsg(
                System.nanoTime(),
                LocalDateTime.now(),
                entry.get(0),
                randomBidPrice,
                getRandomIntFromRange.apply(1000, 50000),
                randomAskPrice,
                getRandomIntFromRange.apply(1000, 50000),
                entry.get(3),
                entry.get(3));
    }

    private static List<String> buildListOfSymbolExchangeAndPrice(
            String symbol, String low, String high, String exchange) {
        List<String> items = new ArrayList<>();
        items.add(symbol);
        items.add(low);
        items.add(high);
        items.add(exchange);

        return items;
    }

    BinaryOperator<Integer> getRandomIntFromRange =
            (x, y) -> {
                Random random = new Random();
                return random.nextInt(y - x) + x;
            };
}
